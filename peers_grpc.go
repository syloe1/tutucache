package geecache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"geecache/consistenthash"
	pb "geecache/geecachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// peers_grpc.go
const (
	defaultGRPCReplicas = 50
)

// gRPC metadata key 必须是小写；与 HTTP 的 SharedTokenHeader 保持同步。
var grpcTokenMetadataKey = strings.ToLower(SharedTokenHeader)

// =============================================================================
// gRPC 服务端 —— 实现 protoc 生成的 GroupCacheServer 接口
// =============================================================================

// GRPCServer 实现了 geecachepb.GroupCacheServer，处理来自其他节点的 gRPC 请求
type GRPCServer struct {
	pb.UnimplementedGroupCacheServer
}

// Get 是 gRPC 服务端的方法：接收远程节点的缓存查询，走本地 Group 的完整查询链路
func (s *GRPCServer) Get(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	group := GetGroup(req.GetGroup())
	if group == nil {
		// Group 未注册属于集群配置错误，用 FailedPrecondition 与「key 不存在」的
		// NotFound 区分，客户端才能正确分类（配置错误 vs 正常未命中）。
		return nil, status.Errorf(codes.FailedPrecondition, "no such group: %s", req.GetGroup())
	}
	view, err := group.Get(req.GetKey())
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			// 正常未命中：客户端不计入熔断，也不会当作配置错误。
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.Response{Value: view.ByteSlice()}, nil
}

// =============================================================================
// gRPC 客户端 —— 实现项目的 PeerGetter 接口
// =============================================================================

// grpcGetter 封装了 protoc 生成的 gRPC 客户端，并适配为 PeerGetter 接口
type grpcGetter struct {
	addr   string
	client pb.GroupCacheClient
	conn   *grpc.ClientConn
	// clientTLSConfig 是建立连接时快照的配置，用于判断 TLS 切换后是否需要重建。
	clientTLSConfig *tls.Config
	// breakers 按 Group 隔离该 peer 的熔断状态；随 getter 生命周期创建和回收。
	breakers peerBreakers
}

// 编译期断言：grpcGetter 实现了 PeerGetter 与 PeerBreakerProvider
var (
	_ PeerGetter          = (*grpcGetter)(nil)
	_ PeerBreakerProvider = (*grpcGetter)(nil)
)

// BreakerFor 返回该 peer 在指定 Group 下的熔断器（实现 PeerBreakerProvider）。
func (g *grpcGetter) BreakerFor(group string) *CircuitBreaker {
	return g.breakers.get(group)
}

// String 返回对端地址，便于日志输出。
func (g *grpcGetter) String() string {
	return g.addr
}

// matches 报告该客户端是否仍使用给定的 TLS 配置。
// token 由 interceptor 每次 RPC 读取，因此不参与连接复用判断。
func (g *grpcGetter) matches(clientTLS *tls.Config) bool {
	return g.clientTLSConfig == clientTLS
}

// Get 适配层：gRPC 风格 (ctx, req) → (*Resp, error)  →  PeerGetter 风格 (in, out) → error
func (g *grpcGetter) Get(in *pb.Request, out *pb.Response) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := g.client.Get(ctx, in)
	if err != nil {
		return err
	}
	// 用 proto.Merge 而非 *out = *resp，避免拷贝 protobuf 内部的 sync.Mutex
	proto.Merge(out, resp)
	return nil
}

// close 关闭底层 gRPC 连接。
// 重复关闭时 grpc 返回 Canceled 状态，这里视为已关闭而不是失败；
// conn 为 nil 时直接返回，兼容测试中手工构造的 getter。
func (g *grpcGetter) close() error {
	if g.conn == nil {
		return nil
	}
	if err := g.conn.Close(); err != nil && status.Code(err) != codes.Canceled {
		return err
	}
	return nil
}

// =============================================================================
// gRPC 节点池 —— 实现项目的 PeerPicker 接口
// =============================================================================
// GRPCPool 管理一组 gRPC 缓存节点（类似 HTTPPool），负责节点发现和负载均衡
type GRPCPool struct {
	self        string                 // 本节点地址，归一化为 "host:port"
	mu          sync.Mutex             // 保护 peers、peersList、getters 与认证/TLS 配置
	peers       *consistenthash.Map    // 一致性哈希环，映射 key → 节点
	peersList   []string               // 上一次 Set 的节点列表（已归一化），用于差集更新哈希环
	grpcGetters map[string]*grpcGetter // 节点地址 → gRPC 客户端
	// grpcServer 在第一次 Serve/ServeListener 时惰性创建：TLS 凭据和
	// interceptor 必须在建 server 时确定，创建后无法替换。
	grpcServer *grpc.Server
	// 服务端 TLS 配置。gRPC server 一旦启动就不能更换证书，因此只能启动前设置。
	serverTLSConfig *tls.Config
	// 客户端 TLS 配置。变更会重建已有 getter（并重置其熔断状态）。
	clientTLSConfig *tls.Config
	sharedToken     string
	started         bool // server 已创建并开始 Serve
	stopped         bool // Shutdown 后置位，阻止后续 Set 建立新连接
}

// 编译期断言：GRPCPool 实现了 PeerPicker
var _ PeerPicker = (*GRPCPool)(nil)

// NewGRPCPool 创建一个 gRPC 节点池，self 为本节点监听地址。
// self 只在这里归一化一次，之后视为不可变。
func NewGRPCPool(self string) *GRPCPool {
	return &GRPCPool{
		self:        normalizeGRPCPeer(self),
		grpcGetters: make(map[string]*grpcGetter),
	}
}

// normalizeGRPCPeer 把 peer 地址归一化为裸 "host:port"。
// 裸地址原样返回；带 scheme 的 URL（http://、https://、grpc:// 等）
// 只保留 host，避免 peers.json 中残留的 HTTP 写法破坏 gRPC 拨号。
func normalizeGRPCPeer(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.Contains(raw, "://") {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	return u.Host
}

// Log 统一日志前缀
func (p *GRPCPool) Log(format string, v ...interface{}) {
	log.Printf("[GRPCPool %s] %s", p.self, fmt.Sprintf(format, v...))
}

// Set 初始化集群中的所有节点，每个节点复用已建立的 gRPC 连接。
// 与上一次的节点列表做差集，关闭并移除已下线节点的连接，防止拓扑变化时泄漏。
func (p *GRPCPool) Set(peers ...string) {
	p.mu.Lock()
	stale := p.setLocked(peers)
	p.mu.Unlock()

	p.closeGetters(stale)
}

// setLocked 是 Set 的主体，调用方必须持有 p.mu。
// 返回需要锁外关闭的旧 getter：关闭连接需要拆解 resolver/balancer，
// 放到锁外可避免阻塞 PickPeer。
func (p *GRPCPool) setLocked(peers []string) map[string]*grpcGetter {
	// Shutdown 之后不再建立新连接，否则连接会在清理之后重新泄漏。
	if p.stopped {
		return nil
	}

	// 归一化、跳空并去重，保留首次出现顺序。
	normalized := make([]string, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		peer = normalizeGRPCPeer(peer)
		if peer == "" {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		normalized = append(normalized, peer)
	}

	// 节点集合和客户端 TLS 配置都没变时直接返回，避免重建哈希环和连接。
	// token 变化不影响连接（interceptor 每次 RPC 读取），因此不参与判断。
	if sameSlice(normalized, p.peersList) && p.gettersCurrentLocked() {
		return nil
	}

	// 按差集增量更新哈希环：只删除下线节点、添加新增节点，避免整环重建。
	p.peers = updateRing(p.peers, defaultGRPCReplicas, p.peersList, normalized)

	oldGetters := p.grpcGetters
	p.grpcGetters = make(map[string]*grpcGetter, len(normalized))
	for _, peer := range normalized {
		if peer == p.self {
			continue // 不需要给自己创建客户端
		}
		// 拓扑变化时复用仍然存在的节点客户端；只有新增节点或 TLS 配置
		// 变化时才重新 dial。
		if getter, ok := oldGetters[peer]; ok && getter.matches(p.clientTLSConfig) {
			p.grpcGetters[peer] = getter
			continue
		}
		if getter := p.newGetterLocked(peer); getter != nil {
			p.grpcGetters[peer] = getter
		}
	}
	p.peersList = append([]string(nil), normalized...)

	stale := make(map[string]*grpcGetter)
	for peer, getter := range oldGetters {
		if p.grpcGetters[peer] != getter {
			stale[peer] = getter
		}
	}
	return stale
}

// gettersCurrentLocked 报告当前 getter 是否已与节点列表和客户端 TLS 配置一致。
// 调用方必须持有 p.mu。
func (p *GRPCPool) gettersCurrentLocked() bool {
	for _, peer := range p.peersList {
		if peer == p.self {
			continue
		}
		getter, ok := p.grpcGetters[peer]
		if !ok || !getter.matches(p.clientTLSConfig) {
			return false
		}
	}
	return true
}

// rebuildGettersLocked 按当前 peersList 和最新 TLS 配置重建全部非 self 连接。
// 调用方必须持有 p.mu；返回需要锁外关闭的旧 getter。
func (p *GRPCPool) rebuildGettersLocked() map[string]*grpcGetter {
	oldGetters := p.grpcGetters
	p.grpcGetters = make(map[string]*grpcGetter, len(p.peersList))
	for _, peer := range p.peersList {
		if peer == p.self {
			continue
		}
		if getter := p.newGetterLocked(peer); getter != nil {
			p.grpcGetters[peer] = getter
		}
	}
	return oldGetters
}

// newGetterLocked 创建到 peer 的 gRPC 客户端。调用方必须持有 p.mu。
// grpc.NewClient 是惰性的，连接错误通常要到第一次 RPC 才暴露。
func (p *GRPCPool) newGetterLocked(peer string) *grpcGetter {
	var creds credentials.TransportCredentials
	if p.clientTLSConfig != nil {
		creds = credentials.NewTLS(p.clientTLSConfig)
	} else {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(
		peer,
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(p.tokenUnaryClientInterceptor),
	)
	if err != nil {
		p.Log("failed to create grpc client for %s: %v", peer, err)
		return nil
	}
	return &grpcGetter{
		addr:            peer,
		client:          pb.NewGroupCacheClient(conn),
		conn:            conn,
		clientTLSConfig: p.clientTLSConfig,
	}
}

// closeGetters 在锁外关闭一组客户端连接。
func (p *GRPCPool) closeGetters(getters map[string]*grpcGetter) {
	for addr, getter := range getters {
		if err := getter.close(); err != nil {
			p.Log("close grpc conn %s failed: %v", addr, err)
			continue
		}
		p.Log("closed peer conn: %s", addr)
	}
}

// SetSharedToken 设置节点间认证 token；空字符串关闭认证。
// token 由 interceptor 在每次 RPC 时读取，因此新值立即对已有连接和
// 运行中的 server 生效，不需要重连。
func (p *GRPCPool) SetSharedToken(token string) {
	p.mu.Lock()
	p.sharedToken = token
	p.mu.Unlock()
}

// authorized 校验请求携带的 token；未配置 token 时不做鉴权。
func (p *GRPCPool) authorized(got string) bool {
	p.mu.Lock()
	want := p.sharedToken
	p.mu.Unlock()
	if want == "" {
		return true
	}
	return tokenEqual(got, want)
}

// EnableTLS 加载证书，启用节点间 TLS 通信；配置语义与 HTTPPool 一致。
// 可在 Set/StartDiscovery 之后调用：已有客户端会立即用新配置重建。
// gRPC server 的凭据创建后不可替换，因此 Serve/ServeListener 之后调用会返回错误。
func (p *GRPCPool) EnableTLS(certFile, keyFile, caCertFile string) error {
	serverCfg, clientCfg, err := buildPeerTLSConfigs(certFile, keyFile, caCertFile)
	if err != nil {
		return err
	}

	p.mu.Lock()
	if p.started || p.stopped {
		p.mu.Unlock()
		return errors.New("cannot enable TLS after gRPC server started or stopped")
	}
	p.serverTLSConfig = serverCfg
	p.clientTLSConfig = clientCfg
	stale := p.rebuildGettersLocked()
	p.mu.Unlock()

	p.closeGetters(stale)
	return nil
}

// TLSConfig 返回服务端 TLS 配置；TLS 未启用时返回 nil。
// 返回的配置视为只读，调用方不得修改。
func (p *GRPCPool) TLSConfig() *tls.Config {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.serverTLSConfig
}

// tokenUnaryClientInterceptor 在每次 RPC 时把当前 token 写入 metadata。
func (p *GRPCPool) tokenUnaryClientInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	p.mu.Lock()
	token := p.sharedToken
	p.mu.Unlock()
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, grpcTokenMetadataKey, token)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// tokenUnaryServerInterceptor 校验入站请求的 token。
// 认证失败返回 Unauthenticated；peererror.go 会把它归类为配置错误。
func (p *GRPCPool) tokenUnaryServerInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	p.mu.Lock()
	want := p.sharedToken
	p.mu.Unlock()
	if want == "" {
		return handler(ctx, req)
	}

	var got string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(grpcTokenMetadataKey); len(vals) > 0 {
			got = vals[0]
		}
	}
	if !tokenEqual(got, want) {
		return nil, status.Error(codes.Unauthenticated, "unauthorized")
	}
	return handler(ctx, req)
}

// serverOptionsLocked 返回创建 gRPC server 所需的选项。
// 调用方必须持有 p.mu。
func (p *GRPCPool) serverOptionsLocked() []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(p.tokenUnaryServerInterceptor),
	}
	if p.serverTLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(p.serverTLSConfig)))
	}
	return opts
}

// StartDiscovery 把本节点注册到注册中心，并在节点列表变化时增量更新拓扑。
func (p *GRPCPool) StartDiscovery(d ServiceDiscovery) error {
	if err := d.Register(p.self); err != nil {
		return err
	}
	d.Watch(func(peers []string) {
		p.Log("discovery: peers updated %v", peers)
		p.Set(peers...)
	})
	return nil
}

// PickPeer 根据 key 选择应访问的远程节点
func (p *GRPCPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.peers == nil {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s for key %s", peer, key)
		if getter, ok := p.grpcGetters[peer]; ok {
			return getter, true
		}
	}
	return nil, false
}

// ServeListener 在给定 listener 上启动 gRPC 服务并阻塞。
// 每次启动都会创建全新的 server，因此必须保证只调用一次。
func (p *GRPCPool) ServeListener(ln net.Listener) error {
	if ln == nil {
		return errors.New("serve grpc: nil listener")
	}

	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		_ = ln.Close()
		return errors.New("grpc pool stopped")
	}
	if p.started {
		p.mu.Unlock()
		_ = ln.Close()
		return errors.New("grpc server already started")
	}
	srv := grpc.NewServer(p.serverOptionsLocked()...)
	pb.RegisterGroupCacheServer(srv, &GRPCServer{})
	p.grpcServer = srv
	p.started = true
	p.mu.Unlock()

	p.Log("gRPC server listening on %s", ln.Addr())
	err := srv.Serve(ln)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Serve 在本节点地址上启动 gRPC 服务并阻塞。
// 监听失败不会把 pool 标记为已启动，修复端口后可以重试。
func (p *GRPCPool) Serve() error {
	p.mu.Lock()
	stopped, started := p.stopped, p.started
	p.mu.Unlock()
	if stopped {
		return errors.New("grpc pool stopped")
	}
	if started {
		return errors.New("grpc server already started")
	}

	ln, err := net.Listen("tcp", p.self)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.self, err)
	}
	p.Log("gRPC server listening on %s", p.self)
	return p.ServeListener(ln)
}

// Shutdown 优雅关闭 gRPC 服务并关闭全部客户端连接。
// ctx 超时后强制 Stop，返回 ctx.Err()；重复调用是幂等的。
//
// GracefulStop 会等待处理中的 handler，而 handler 可能还要通过客户端
// 连接转发到其他节点，因此客户端连接要等 GracefulStop 返回后再关闭。
func (p *GRPCPool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	srv := p.grpcServer
	p.mu.Unlock()

	var shutdownErr error
	if srv != nil {
		done := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			// 仍有请求未完成：强制终止，不再无限等待。
			srv.Stop()
			<-done
			shutdownErr = ctx.Err()
		}
	}

	p.mu.Lock()
	getters := p.grpcGetters
	p.grpcGetters = make(map[string]*grpcGetter)
	p.mu.Unlock()
	p.closeGetters(getters)

	return shutdownErr
}

// Stop 同步关闭 gRPC 服务与全部客户端连接。
func (p *GRPCPool) Stop() {
	_ = p.Shutdown(context.Background())
}
