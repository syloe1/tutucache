package geecache

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50 //虚拟节点数量
	// SharedTokenHeader 节点间认证 token 的 HTTP 头
	SharedTokenHeader = "X-Geecache-Token" //http请求头

	// HTTP 客户端连接池参数。每个 peer 一个 transport，显式设置空闲连接
	// 上限和回收时间，避免节点失联或拓扑变化后 keep-alive 连接无限堆积。
	idleConnTimeout     = 90 * time.Second
	maxIdleConns        = 100
	maxIdleConnsPerHost = 32
)

// PeerHTTPError 表示对端返回了非 200 的 HTTP 状态码。
// 调用方可用 errors.As 区分配置错误和远端临时故障。
type PeerHTTPError struct {
	StatusCode int
}

func (e *PeerHTTPError) Error() string {
	return fmt.Sprintf("peer returned HTTP %d %s", e.StatusCode, http.StatusText(e.StatusCode))
}

// IsConfigError 表示集群配置不一致（鉴权失败或对端未注册该 group），
// 这类错误应暴露给调用方，禁止静默降级到本地。
func (e *PeerHTTPError) IsConfigError() bool {
	switch e.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return true
	default:
		return false
	}
}

// isPeerConfigError 判断是否为集群配置错误（401/403/404）
func isPeerConfigError(err error) bool {
	var peerErr *PeerHTTPError
	return errors.As(err, &peerErr) && peerErr.IsConfigError()
}

// =============================================================================
// HTTPPool — 节点间 HTTP 通信（服务端 + 客户端管理）
// =============================================================================

type HTTPPool struct {
	self        string                 // 当前节点的地址，例如 "http://127.0.0.1:8001"
	basePath    string                 // 路由前缀 /_geecache/
	mu          sync.Mutex             // 保护 peers、httpGetters 与 TLS/token 配置
	peers       *consistenthash.Map    // 一致性哈希环，保存所有集群节点
	peersList   []string               // 上一次 Set 的节点列表（已按当前 scheme 归一化）
	httpGetters map[string]*httpGetter // key:节点地址，value:该节点的http客户端
	// 服务端 TLS 配置，挂到本地 http.Server。EnableTLS 返回后视为只读。
	serverTLSConfig *tls.Config
	// 客户端 TLS 配置，供访问 peer 的 httpGetter 使用。非 nil 表示节点间通信走 https。
	clientTLSConfig *tls.Config
	sharedToken     string
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// EnableTLS 加载证书，启用节点间 TLS 通信。
// caCertFile 为空时只加密、不要求客户端证书；非空时开启 mTLS：
// 服务端要求并校验客户端证书，客户端用同一个 CA 校验服务端证书。
//
// 同一个证书同时用于服务端和客户端角色，因此证书 SAN 必须覆盖 peer 实际
// 拨号的地址；启用 mTLS 时证书还需要 clientAuth EKU。
//
// 必须在 StartDiscovery/Set 之前调用。返回后两份配置都视为只读。
func (p *HTTPPool) EnableTLS(certFile, keyFile, caCertFile string) error {
	// 加载节点证书 + 私钥对
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load cert: %w", err)
	}

	// 服务端和客户端角色必须使用两份独立的配置：
	// 服务端需要 ClientCAs + ClientAuth 校验客户端证书；
	// 客户端需要 RootCAs 校验服务端证书。
	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	clientCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if caCertFile != "" {
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return fmt.Errorf("read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return errors.New("append CA cert: invalid PEM")
		}
		// 服务端用 ClientCAs 校验客户端证书。
		serverCfg.ClientCAs = caCertPool
		serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
		// 客户端用 RootCAs 校验远端节点的服务端证书。
		clientCfg.RootCAs = caCertPool
	}

	p.mu.Lock()
	p.serverTLSConfig = serverCfg
	p.clientTLSConfig = clientCfg
	p.mu.Unlock()
	return nil
}

// SetSharedToken 设置共享 Token（最简单内网认证方式）。
// 必须在 StartDiscovery/Set 之前调用。
func (p *HTTPPool) SetSharedToken(token string) {
	p.mu.Lock()
	p.sharedToken = token
	p.mu.Unlock()
}

// TLSConfig 返回服务端 TLS 配置（供 main.go 的 http.Server 使用）。
// TLS 未启用时返回 nil；返回的配置视为只读，调用方不得修改。
func (p *HTTPPool) TLSConfig() *tls.Config {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.serverTLSConfig
}

func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// =============================================================================
// 服务端
// =============================================================================

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// /health 和 /metrics 不校验 token
	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		return
	}
	if r.URL.Path == "/metrics" {
		MetricsHandler().ServeHTTP(w, r)
		return
	}

	// 节点间通信 — 校验共享 token
	if p.sharedToken != "" && r.Header.Get(SharedTokenHeader) != p.sharedToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	//`basePath` 一般是 `/geecache/`，限定本 handler 只处理 `/geecache/` 开头的请求。
	// 必须用 EscapedPath 而不是 Path：Path 已被整体解码，key 中的 %2F 会变成 "/"，
	// 无法再与路径分隔符区分。正确做法是先按 "/" 切段，再逐段 PathUnescape。
	escapedPath := r.URL.EscapedPath()
	if !strings.HasPrefix(escapedPath, p.basePath) {
		// ServeHTTP 是整个节点的根 handler，扫描器或健康检查随时可能请求任意路径；
		// 这里不能用 panic 做路由控制，否则每个请求都会打出一份堆栈并重置连接。
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.Log("%s %s", r.Method, escapedPath)
	// 把前缀切掉
	parts := strings.SplitN(escapedPath[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	//`p.basePath = "/geecache/"`
	//收到请求路径：`/geecache/user/1000`
	groupName, err := url.PathUnescape(parts[0]) //user
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	key, err := url.PathUnescape(parts[1]) // 1000
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	// 整个GeeCache,别人想查缓存， 统一调用这个方法
	// 输入key, 返回缓存数据ByteVie, 找不到报错
	view, err := group.Get(key)
	if err != nil {
		// 详细错误只写服务端日志，不返回给客户端
		p.Log("get key %q failed: %v", key, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	//Proto序列化
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		p.Log("marshal response for key %q failed: %v", key, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(body)
}

// =============================================================================
// 节点管理
// =============================================================================
// 初始化集群节点列表， 构建一致性哈希环
// `peers ...string` 是 Go 可变参数，可以一次性传入多个节点地址字符串。
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 按当前 TLS 模式统一 peer 的 scheme，并去重：
	// peers.json 中可能同时残留 http:// 与 https:// 两种写法，归一化后会重复。
	// 集群要求所有节点使用一致的 TLS 配置，因此以本节点的模式为准。
	scheme := p.peerScheme()
	self := normalizePeerURL(p.self, scheme)
	normalized := make([]string, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" {
			continue
		}
		peer = normalizePeerURL(peer, scheme)
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		normalized = append(normalized, peer)
	}
	peers = normalized

	// FileDiscovery 每 3 秒回调一次。节点集合和认证/TLS 配置都没变时直接返回，
	// 避免周期性地重建哈希环和全部 transport（重建会让 keep-alive 连接失去复用）。
	if sameSlice(peers, p.peersList) && p.gettersCurrent() {
		return
	}

	//创建一致性哈希环
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...) //把所有集群真实节点，加到哈希环上

	oldGetters := p.httpGetters
	//key是节点地址字符串， value是访问远程节点的HTTP客户端
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		if peer == self {
			continue
		}
		// 拓扑变化时复用仍然存在的节点客户端：幸存节点的 transport 和
		// keep-alive 连接可以继续使用，只有新增节点才需要新建客户端。
		if getter, ok := oldGetters[peer]; ok && getter.matches(p.clientTLSConfig, p.sharedToken) {
			p.httpGetters[peer] = getter
			continue
		}
		// peer 已经按当前 TLS 模式归一化为 http:// 或 https://，再拼上 basePath。
		p.httpGetters[peer] = newHTTPGetter(peer+p.basePath, p.clientTLSConfig, p.sharedToken)
	}
	p.peersList = append([]string(nil), peers...)

	// 被移除（或配置已变化）的旧客户端不再被使用，立即关闭其空闲 keep-alive
	// 连接；否则每次拓扑变化都会残留一批永远不会被复用的 ESTABLISHED 连接。
	for peer, getter := range oldGetters {
		if p.httpGetters[peer] != getter {
			getter.close()
		}
	}
}

// peerScheme 返回当前节点间通信使用的 URL scheme。
// 调用方必须持有 p.mu。
func (p *HTTPPool) peerScheme() string {
	if p.clientTLSConfig != nil {
		return "https"
	}
	return "http"
}

// normalizePeerURL 把 peer 地址的 scheme 改成当前节点间通信使用的 scheme。
// 地址不合法或缺少 host 时原样返回，交给请求阶段报错。
func normalizePeerURL(raw, scheme string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	u.Scheme = scheme
	return u.String()
}

// gettersCurrent 报告当前 httpGetters 是否已经与节点列表、TLS 和 token 配置一致。
// 调用方必须持有 p.mu。
func (p *HTTPPool) gettersCurrent() bool {
	self := normalizePeerURL(p.self, p.peerScheme())
	for _, peer := range p.peersList {
		if peer == self {
			continue
		}
		getter, ok := p.httpGetters[peer]
		if !ok || !getter.matches(p.clientTLSConfig, p.sharedToken) {
			return false
		}
	}
	return true
}

func (p *HTTPPool) StartDiscovery(d ServiceDiscovery) error {
	// 当前节点自己注册到注册中心
	if err := d.Register(p.self); err != nil {
		return err
	}
	// 持续阻塞监听注册中心， 一旦节点列表发生编号， 就执行回调函数
	d.Watch(func(peers []string) {
		p.Log("discovery: peers updated %v", peers)
		p.Set(peers...)
	})
	return nil
}

// 根据key， 挑选这个key归属的远程节点， 返回能访问该节点的客户端PeerGetter
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.peers == nil {
		return nil, false
	}
	self := normalizePeerURL(p.self, p.peerScheme())
	if peer := p.peers.Get(key); peer != "" && peer != self {
		p.Log("Pick peer %s", peer)
		//取客户端
		if getter, ok := p.httpGetters[peer]; ok {
			return getter, true
		}
	}
	return nil, false
}

var _ PeerPicker = (*HTTPPool)(nil)

// =============================================================================
// HTTP 客户端 — 实现 PeerGetter 接口
// =============================================================================
// 远程节点HTTP客户端
type httpGetter struct {
	baseURL         string
	httpClient      *http.Client
	sharedToken     string
	clientTLSConfig *tls.Config
}

// newHTTPGetter 创建客户端（包内使用）
func newHTTPGetter(baseURL string, tlsCfg *tls.Config, sharedToken string) *httpGetter {
	//`http.Transport`：**HTTP 底层传输层**
	// 如果传入 TLS 配置，HTTPS 握手时使用这套证书配置，开启 TLS/mTLS 加密
	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     idleConnTimeout,
	}
	return &httpGetter{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   2 * time.Second,
		},
		sharedToken:     sharedToken,
		clientTLSConfig: tlsCfg,
	}
}

// matches 报告该客户端是否仍使用给定的 TLS/token 配置。
func (g *httpGetter) matches(clientTLS *tls.Config, token string) bool {
	return g.clientTLSConfig == clientTLS && g.sharedToken == token
}

// close 关闭客户端的空闲 keep-alive 连接。正在进行的请求不受影响。
func (h *httpGetter) close() {
	h.httpClient.CloseIdleConnections()
}

// String 返回对端地址，避免日志打印出 sharedToken 等内部字段。
func (h *httpGetter) String() string {
	return h.baseURL
}

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		// group 和 key 是独立的 path segment，必须用 PathEscape：
		// QueryEscape 会把空格编码成 "+"，而服务端路径中的 "+" 是字面加号。
		url.PathEscape(in.GetGroup()),
		url.PathEscape(in.GetKey()),
	)
	// 构造Get请求
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return fmt.Errorf("build peer http request: %w", err)
	}
	if h.sharedToken != "" {
		// 添加 kv键值对
		req.Header.Set(SharedTokenHeader, h.sharedToken)
	}
	// 发起网络请求， 阻塞等待远端B节点返回HTTP响应
	res, err := h.httpClient.Do(req)
	if err != nil {
		// 连接超时， 连接拒绝， DNS失败，返回普通Error
		return fmt.Errorf("http get peer: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return &PeerHTTPError{
			StatusCode: res.StatusCode,
		}
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading peer response body: %w", err)
	}
	//反序列化
	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("unmarshal peer protobuf response: %w", err)
	}
	return nil
}

var _ PeerGetter = (*httpGetter)(nil)
