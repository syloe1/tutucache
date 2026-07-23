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
	defaultBasePath   = "/_geecache/"
	defaultReplicas   = 50
	// SharedTokenHeader 节点间认证 token 的 HTTP 头
	SharedTokenHeader = "X-Geecache-Token"
)

// =============================================================================
// HTTPPool — 节点间 HTTP 通信（服务端 + 客户端管理）
// =============================================================================

type HTTPPool struct {
	self     string
	basePath string
	mu       sync.Mutex

	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter

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
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load cert: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if caCertFile != "" {
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return fmt.Errorf("read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
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

	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)

	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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

func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)

	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		if peer == p.self {
			continue
		}
		p.httpGetters[peer] = newHTTPGetter(peer+p.basePath, p.tlsConfig, p.sharedToken)
	}
}

func (p *HTTPPool) StartDiscovery(d ServiceDiscovery) error {
	if err := d.Register(p.self); err != nil {
		return err
	}
	d.Watch(func(peers []string) {
		p.Log("discovery: peers updated %v", peers)
		p.Set(peers...)
	})
	return nil
}

func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.peers == nil {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
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

type httpGetter struct {
	baseURL     string
	httpClient  *http.Client
	sharedToken string
}

// newHTTPGetter 创建客户端（包内使用）
func newHTTPGetter(baseURL string, tlsCfg *tls.Config, sharedToken string) *httpGetter {
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
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	if h.sharedToken != "" {
		req.Header.Set(SharedTokenHeader, h.sharedToken)
	}

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

	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

var _ PeerGetter = (*httpGetter)(nil)
