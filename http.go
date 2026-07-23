package geecache

import (
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
)

// 提供被其他节点访问的能力
const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

// 做承载节点间HTTP通信的核心结构
type HTTPPool struct {
	self     string
	basePath string
	mu       sync.Mutex

	peers *consistenthash.Map
	/*
		Map 一致性哈希环
		type Map struct {
			hash     Hash           // 哈希函数
			replicas int            // 每个真实节点对应的虚拟节点数量
			keys     []int          // 哈希环上所有虚拟节点哈希值，升序排列
			hashMap  map[int]string // 虚拟节点hash -> 真实节点名
		}
	*/
	httpGetters map[string]*httpGetter //远程节点和对应的URL
	/*
		type httpGetter struct {
			baseURL string
		}
	*/
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,            //记录 主机名/IP + 端口
		basePath: defaultBasePath, //节点间通讯地址的前缀
	}
}

/*
	type Handler interface{
		ServeHTTP(w ResponseWriter, r *Request)
	}
*/
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// 注册进http.Server，让它处理HTTP请求
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// /health 端点 — 健康检查
	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		return
	}

	// /metrics 端点 — 暴露缓存指标
	if r.URL.Path == "/metrics" {
		MetricsHandler().ServeHTTP(w, r)
		return
	}

	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)
	// /<basepath>/<groupname>/<key> required
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
	//把ByteView打包成pb.Response， 序列化为二进制
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(body)
}

func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	//创建哈希环
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{
			baseURL: peer + p.basePath,
		}
	}
}

// 返回节点对应的客户端
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

/*
	type PeerPicker interface {
		PickPeer(key string) (peer PeerGetter, ok bool)
	}
*/
//编译器断言， 强制检查*HTTPPool是否实现了PeerPicker接口
var _ PeerPicker = (*HTTPPool)(nil)

// HTTP客户端
type httpGetter struct {
	baseURL string
}

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	res, err := http.Get(u)
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
	//stage 7, add protobuf //把二进制反序列化为pb.Response
	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", res)
	}
	return nil
}

// 编译期断言， 强制检查httpGetter是否实现了PeerGetter接口
var _ PeerGetter = (*httpGetter)(nil)
