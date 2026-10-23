package geecache

import (
	"fmt"
	pb "geecache/geecachepb"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// trackingTransport 只用于观察 CloseIdleConnections 是否被调用。
type trackingTransport struct {
	closeIdle atomic.Int32
}

func (t *trackingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil // 这些测试不会真正发请求
}

func (t *trackingTransport) CloseIdleConnections() {
	t.closeIdle.Add(1)
}

func replaceTransport(g *httpGetter) *trackingTransport {
	tr := &trackingTransport{}
	g.httpClient.Transport = tr
	return tr
}

func TestHTTPGetterTransportSettings(t *testing.T) {
	getter := newHTTPGetter("http://peer"+defaultBasePath, nil, "token")

	transport, ok := getter.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", getter.httpClient.Transport)
	}
	if transport.IdleConnTimeout != idleConnTimeout {
		t.Errorf("IdleConnTimeout = %v, want %v", transport.IdleConnTimeout, idleConnTimeout)
	}
	if transport.MaxIdleConns != maxIdleConns {
		t.Errorf("MaxIdleConns = %d, want %d", transport.MaxIdleConns, maxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != maxIdleConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost = %d, want %d", transport.MaxIdleConnsPerHost, maxIdleConnsPerHost)
	}
}

// 回归测试 #10：FileDiscovery 周期性以相同节点集合调用 Set 时不能重建客户端，
// 否则每次轮询都会丢弃带 keep-alive 连接的 transport。
func TestHTTPPoolSetUnchangedPeersKeepsGetters(t *testing.T) {
	p := NewHTTPPool("http://self")
	p.Set("http://a", "http://b")

	getterA := p.httpGetters["http://a"]
	getterB := p.httpGetters["http://b"]
	trA := replaceTransport(getterA)
	trB := replaceTransport(getterB)

	p.Set("http://b", "http://a") // 集合相同，顺序不同

	if got := p.httpGetters["http://a"]; got != getterA {
		t.Fatal("unchanged peer set rebuilt the getter for http://a")
	}
	if got := p.httpGetters["http://b"]; got != getterB {
		t.Fatal("unchanged peer set rebuilt the getter for http://b")
	}
	if n := trA.closeIdle.Load(); n != 0 {
		t.Errorf("closeIdle called %d times for surviving peer http://a", n)
	}
	if n := trB.closeIdle.Load(); n != 0 {
		t.Errorf("closeIdle called %d times for surviving peer http://b", n)
	}
}

// 回归测试 #10：移除节点时必须关闭被替换 transport 的空闲连接，
// 同时幸存节点的客户端（及其 keep-alive 连接）应被复用。
func TestHTTPPoolSetClosesRemovedGetter(t *testing.T) {
	p := NewHTTPPool("http://self")
	p.Set("http://a", "http://b")

	trA := replaceTransport(p.httpGetters["http://a"])
	getterB := p.httpGetters["http://b"]
	trB := replaceTransport(getterB)

	p.Set("http://b")

	if n := trA.closeIdle.Load(); n != 1 {
		t.Errorf("closeIdle called %d times for removed peer http://a, want 1", n)
	}
	if n := trB.closeIdle.Load(); n != 0 {
		t.Errorf("closeIdle called %d times for surviving peer http://b, want 0", n)
	}
	if got := p.httpGetters["http://b"]; got != getterB {
		t.Error("surviving peer http://b got a new getter")
	}
	if _, ok := p.httpGetters["http://a"]; ok {
		t.Error("removed peer http://a still has a getter")
	}
}

// 即使节点集合不变，认证配置变化后也必须重建客户端，否则会一直使用旧 token。
// 回归测试 #13：group/key 作为独立 path segment 编码，含空格、/、+、中文和
// 字面 %xx 的 key 必须经完整 HTTP 往返后原样返回。
// 每个 key 使用独立 group，避免某个 key 的错误结果被缓存后污染后续断言。
func TestPeerHTTPKeyRoundTrip(t *testing.T) {
	keys := []string{"plain", "a b", "a/b", "a+b", "你好", "a%2Fb", "a?b", "a#b"}
	for i, key := range keys {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			group := fmt.Sprintf("roundtrip-%d", i)
			NewGroup(group, 1<<20, GetterFunc(func(k string) ([]byte, error) {
				return []byte(k), nil
			}))

			pool := NewHTTPPool("http://self")
			srv := httptest.NewServer(pool)
			defer srv.Close()

			getter := newHTTPGetter(srv.URL+defaultBasePath, nil, "")
			out := &pb.Response{}
			if err := getter.Get(&pb.Request{Group: group, Key: key}, out); err != nil {
				t.Fatalf("get key %q: %v", key, err)
			}
			if got := string(out.Value); got != key {
				t.Errorf("key %q round-tripped as %q", key, got)
			}
		})
	}
}

func TestHTTPPoolSetRebuildsOnConfigChange(t *testing.T) {
	p := NewHTTPPool("http://self")
	p.Set("http://a")

	oldGetter := p.httpGetters["http://a"]
	tr := replaceTransport(oldGetter)

	p.SetSharedToken("secret")
	p.Set("http://a")

	newGetter := p.httpGetters["http://a"]
	if newGetter == oldGetter {
		t.Fatal("getter was not rebuilt after token change")
	}
	if newGetter.sharedToken != "secret" {
		t.Errorf("getter token = %q, want %q", newGetter.sharedToken, "secret")
	}
	if n := tr.closeIdle.Load(); n != 1 {
		t.Errorf("closeIdle called %d times for stale getter, want 1", n)
	}
}
