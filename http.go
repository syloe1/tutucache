package geecache

import (
	"crypto/tls"
	"crypto/x509"
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

	"github.com/golang/protobuf/proto"
)

const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50 //虚拟节点数量
	// SharedTokenHeader 节点间认证 token 的 HTTP 头
	SharedTokenHeader = "X-Geecache-Token" //http请求头
)

// =============================================================================
// HTTPPool — 节点间 HTTP 通信（服务端 + 客户端管理）
// =============================================================================

type HTTPPool struct {
	self        string                 // 当前节点的地址，例如 "http://127.0.0.1:8001"
	basePath    string                 // 路由前缀 /_geecache/
	mu          sync.Mutex             // 保护 peers、httpGetters 的并发锁
	peers       *consistenthash.Map    // 一致性哈希环，保存所有集群节点
	httpGetters map[string]*httpGetter // key:节点地址，value:该节点的http客户端
	// TLS / 认证配置（可选）
	tlsConfig   *tls.Config
	sharedToken string
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// EnableTLS 加载证书，启用节点间 TLS 通信
// caCertFile 为空时只加密不校验客户端证书；非空时开启 mTLS
func (p *HTTPPool) EnableTLS(certFile, keyFile, caCertFile string) error {
	// 加载服务端证书 + 私钥对
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load cert: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert}, //配置服务端证书列表
		MinVersion:   tls.VersionTLS12,
	}

	if caCertFile != "" {
		// 如果传入CA证书路径，开启双向 mTLS
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return fmt.Errorf("read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()      // 创建CA证书池，可以存放多个可信根证书
		caCertPool.AppendCertsFromPEM(caCert) // 把CA证书加入证书池
		cfg.RootCAs = caCertPool
	}

	p.tlsConfig = cfg
	return nil
}

// SetSharedToken 设置共享 Token（最简单内网认证方式）
func (p *HTTPPool) SetSharedToken(token string) {
	p.sharedToken = token
}

// TLSConfig 返回服务端 TLS 配置（供 main.go 的 http.Server 使用）
func (p *HTTPPool) TLSConfig() *tls.Config {
	return p.tlsConfig
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
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)
	// 把前缀切掉
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	//`p.basePath = "/geecache/"`
	//收到请求路径：`/geecache/user/1000`
	groupName := parts[0] //user
	key := parts[1]       // 1000

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	// 整个GeeCache,别人想查缓存， 统一调用这个方法
	// 输入key, 返回缓存数据ByteVie, 找不到报错
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//Proto序列化
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	//创建一致性哈希环
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...) //把所有集群真实节点，加到哈希环上
	//key是节点地址字符串， value是访问远程节点的HTTP客户端
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		if peer == p.self {
			continue
		}
		//peer 是`http://127.0.0.1:8001`，basePath 是`/geecache/`，得到 `http://127.0.0.1:8001/geecache/`
		p.httpGetters[peer] = newHTTPGetter(peer+p.basePath, p.tlsConfig, p.sharedToken)
	}
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
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
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
	baseURL     string
	httpClient  *http.Client
	sharedToken string
}

// newHTTPGetter 创建客户端（包内使用）
func newHTTPGetter(baseURL string, tlsCfg *tls.Config, sharedToken string) *httpGetter {
	//`http.Transport`：**HTTP 底层传输层**
	// 如果传入 TLS 配置，HTTPS 握手时使用这套证书配置，开启 TLS/mTLS 加密
	transport := &http.Transport{
		TLSClientConfig: tlsCfg,
	}
	return &httpGetter{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   2 * time.Second,
		},
		sharedToken: sharedToken,
	}
}

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		//对 group 名和 key 做 URL 转义编码
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	// 构造Get请求
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	if h.sharedToken != "" {
		// 添加 kv键值对
		req.Header.Set(SharedTokenHeader, h.sharedToken)
	}
	// 发起网络请求， 阻塞等待远端B节点返回HTTP响应
	res, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	//反序列化
	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

var _ PeerGetter = (*httpGetter)(nil)
