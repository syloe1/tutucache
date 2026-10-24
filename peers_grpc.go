package geecache

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"geecache/consistenthash"
	pb "geecache/geecachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// peers_grpc.go
const (
	defaultGRPCReplicas = 50
)

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
		return nil, status.Errorf(codes.NotFound, "no such group: %s", req.GetGroup())
	}
	view, err := group.Get(req.GetKey())
	if err != nil {
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
}

// 编译期断言：grpcGetter 实现了 PeerGetter
var _ PeerGetter = (*grpcGetter)(nil)

// String 返回对端地址，便于日志输出。
func (g *grpcGetter) String() string {
	return g.addr
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
	self        string                 // 本节点地址，如 "localhost:8001"
	mu          sync.Mutex             // 保护 peers、peersList 和 grpcGetters
	peers       *consistenthash.Map    // 一致性哈希环，映射 key → 节点
	peersList   []string               // 上一次 Set 的节点列表，用于差集更新哈希环
	grpcGetters map[string]*grpcGetter // 节点地址 → gRPC 客户端
	grpcServer  *grpc.Server           // 本节点的 gRPC server
	stopped     bool                   // Stop 后置位，阻止后续 Set 重新建立客户端连接
}

// 编译期断言：GRPCPool 实现了 PeerPicker
var _ PeerPicker = (*GRPCPool)(nil)

// NewGRPCPool 创建一个 gRPC 节点池，self 为本节点监听地址
func NewGRPCPool(self string) *GRPCPool {
	return &GRPCPool{
		self:        self,
		grpcGetters: make(map[string]*grpcGetter),
		grpcServer:  grpc.NewServer(),
	}
}

// Log 统一日志前缀
func (p *GRPCPool) Log(format string, v ...interface{}) {
	log.Printf("[GRPCPool %s] %s", p.self, fmt.Sprintf(format, v...))
}

// Set 初始化集群中的所有节点，每个节点复用已建立的 gRPC 连接。
// 与上一次的节点列表做差集，关闭并移除已下线节点的连接，防止拓扑变化时泄漏。
func (p *GRPCPool) Set(peers ...string) {
	p.mu.Lock()
	// Stop 之后不再建立新连接，否则连接会在 Stop 清理之后重新泄漏。
	if p.stopped {
		p.mu.Unlock()
		return
	}

	// 去重并保留首次出现顺序；newPeerSet 同时用于 getter 差集判断，
	// 避免重复节点被重复加入哈希环。
	newPeerSet := make(map[string]struct{}, len(peers))
	unique := make([]string, 0, len(peers))
	for _, peer := range peers {
		if _, ok := newPeerSet[peer]; ok {
			continue
		}
		newPeerSet[peer] = struct{}{}
		unique = append(unique, peer)
	}
	peers = unique
	// 找出旧 getter 中不在新列表里的 peer：锁内只摘除，锁外再关闭。
	// 关闭连接需要拆解 resolver/balancer，放到锁外可避免阻塞 PickPeer。
	stale := make(map[string]*grpcGetter)
	for addr, getter := range p.grpcGetters {
		if _, ok := newPeerSet[addr]; !ok {
			stale[addr] = getter
			delete(p.grpcGetters, addr)
		}
	}

	// 按差集增量更新哈希环：只删除下线节点、添加新增节点，避免整环重建。
	p.peers = updateRing(p.peers, defaultGRPCReplicas, p.peersList, peers)
	p.peersList = append([]string(nil), peers...)

	// 为每个对端节点创建 gRPC 客户端（复用连接）
	for _, peer := range peers {
		if peer == p.self {
			continue // 不需要给自己创建客户端
		}
		if _, ok := p.grpcGetters[peer]; ok {
			continue // 已存在，跳过
		}
		conn, err := grpc.NewClient(
			peer,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			p.Log("failed to dial %s: %v", peer, err)
			continue
		}
		p.grpcGetters[peer] = &grpcGetter{
			addr:   peer,
			client: pb.NewGroupCacheClient(conn),
			conn:   conn,
		}
	}
	p.mu.Unlock()

	// 锁外关闭已下线节点的连接
	for addr, getter := range stale {
		if err := getter.close(); err != nil {
			p.Log("close stale grpc conn %s failed: %v", addr, err)
			continue
		}
		p.Log("closed stale peer conn: %s", addr)
	}
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

// Serve 启动 gRPC 服务，注册本节点为 GroupCacheServer，阻塞直到出错
func (p *GRPCPool) Serve() error {
	// 将本节点注册为 gRPC GroupCacheServer
	pb.RegisterGroupCacheServer(p.grpcServer, &GRPCServer{})

	lis, err := net.Listen("tcp", p.self)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.self, err)
	}

	p.Log("gRPC server listening on %s", p.self)
	return p.grpcServer.Serve(lis)
}

// Stop 优雅关闭 gRPC 服务，并关闭全部客户端连接
func (p *GRPCPool) Stop() {
	// 先停止接收并 drain 入站请求，不持 p.mu：正在处理的 handler 可能还需要
	// 通过客户端连接转发到其他节点，因此此时不能先关闭客户端连接。
	p.grpcServer.GracefulStop()

	// 锁内只做快照和清空，不做阻塞 I/O
	p.mu.Lock()
	p.stopped = true
	getters := p.grpcGetters
	p.grpcGetters = make(map[string]*grpcGetter)
	p.mu.Unlock()

	// 锁外关闭
	for addr, getter := range getters {
		if err := getter.close(); err != nil {
			p.Log("close client conn %s on stop failed: %v", addr, err)
		}
	}
}
