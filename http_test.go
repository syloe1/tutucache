package geecache

import (
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
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

// 回归：拓扑变化时只按差集增删节点，不重建整环。
func TestHTTPPoolSetUpdatesRingIncrementally(t *testing.T) {
	p := NewHTTPPool("http://self")
	p.Set("http://a", "http://b", "http://c")
	ring := p.peers

	p.Set("http://a", "http://c", "http://d")

	if p.peers != ring {
		t.Fatal("ring was rebuilt instead of updated in place")
	}

	// 增量更新后的路由结果应与用同样节点重建的环一致。
	ref := consistenthash.New(defaultReplicas, nil)
	ref.Add("http://a", "http://c", "http://d")
	for i := 0; i < 1000; i++ {
		key := "key-" + strconv.Itoa(i)
		got, want := p.peers.Get(key), ref.Get(key)
		if got == "http://b" {
			t.Fatalf("key %q still routes to removed peer http://b", key)
		}
		if got != want {
			t.Fatalf("key %q routes to %q, want %q", key, got, want)
		}
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

// 回归测试 #12：未知路径返回 404，不再 panic。
func TestServeHTTPUnknownPathReturnsNotFound(t *testing.T) {
	p := NewHTTPPool("http://self")
	for _, tc := range []struct{ name, path string }{
		{"root", "/"},
		{"favicon", "/favicon.ico"},
		{"robots", "/robots.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			// 直接调用 ServeHTTP：若仍 panic，测试会直接失败；
			// 走 httptest.NewServer 会被 net/http recover 成连接重置，把问题掩盖掉。
			p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
			}
		})
	}
}

// 回归测试 #12：缓存端点只允许 GET；非 GET 不能触发 getter。
func TestServeHTTPRejectsNonGETOnCachePath(t *testing.T) {
	var calls atomic.Int32
	// group 名要唯一，避免和别的测试互相覆盖全局 groups。
	NewGroup("http-method-check", 1<<20, GetterFunc(func(string) ([]byte, error) {
		calls.Add(1)
		return []byte("v"), nil
	}))

	p := NewHTTPPool("http://self")
	for _, method := range []string{
		http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch,
	} {
		t.Run(method, func(t *testing.T) {
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, httptest.NewRequest(method,
				"/_geecache/http-method-check/k", nil))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
			}
			// RFC 9110 要求 405 响应必须带 Allow 头。
			if got := rec.Header().Get("Allow"); got != http.MethodGet {
				t.Errorf("Allow = %q, want %q", got, http.MethodGet)
			}
		})
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("getter called %d times for non-GET requests, want 0", n)
	}
}

// 回归测试 #12：方法检查不能误伤正常的 GET 读路径。
func TestServeHTTPCacheEndpointGetOK(t *testing.T) {
	const (
		testGroup = "http-get-ok"
		testKey   = "hello"
		testValue = "geecache-value-123"
	)
	NewGroup(testGroup, 1<<10, GetterFunc(func(key string) ([]byte, error) {
		if key == testKey {
			return []byte(testValue), nil
		}
		return []byte("default-" + key), nil
	}))

	p := NewHTTPPool("http://self")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, defaultBasePath+testGroup+"/"+testKey, nil)

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %q", rec.Code, http.StatusOK, rec.Body.String())
	}

	// 解码 proto body
	var resp pb.Response
	if err := proto.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if string(resp.Value) != testValue {
		t.Fatalf("value = %q, want %q", string(resp.Value), testValue)
	}
}

// 回归 #17：key 不存在时返回 410 Gone（而不是 404/500）。
// 404 保留给「Group 未注册」这种真正的配置错误，客户端才能区分两者。
func TestServeHTTPKeyNotFoundReturnsGone(t *testing.T) {
	const testGroup = "http-key-not-found"
	NewGroup(testGroup, 1<<10, GetterFunc(func(key string) ([]byte, error) {
		return nil, fmt.Errorf("missing %s: %w", key, ErrKeyNotFound)
	}))

	p := NewHTTPPool("http://self")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, defaultBasePath+testGroup+"/k", nil)

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d, want %d; body = %q", rec.Code, http.StatusGone, rec.Body.String())
	}
}

// 回归测试 #12：认证先于路由和方法检查——配置 token 后，
// 未认证请求一律 401，认证通过后未知路径才是 404、非 GET 才是 405。
func TestServeHTTPTokenOrder(t *testing.T) {
	const fakeToken = "secret-token-xyz"

	// 配置 token，不带 header：连 unknown 路径都先吃 401（认证优先于路由）
	t.Run("no_token_header_rejects_unknown_path_with_401", func(t *testing.T) {
		p := NewHTTPPool("http://self")
		p.SetSharedToken(fakeToken)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/some/unknown/path", nil)
		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d (auth must run before routing)", rec.Code, http.StatusUnauthorized)
		}
	})

	// 配置 token + 正确 header：unknown 路径返回 404，POST 缓存路径返回 405
	t.Run("correct_token_allows_routing_then_404_and_405", func(t *testing.T) {
		p := NewHTTPPool("http://self")
		p.SetSharedToken(fakeToken)

		// unknown path → 404
		rec1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodGet, "/some/unknown/path", nil)
		req1.Header.Set(SharedTokenHeader, fakeToken)
		p.ServeHTTP(rec1, req1)
		if rec1.Code != http.StatusNotFound {
			t.Fatalf("unknown path status = %d, want %d", rec1.Code, http.StatusNotFound)
		}

		// POST /_geecache/g/k → 405
		rec2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, defaultBasePath+"somegroup/somekey", nil)
		req2.Header.Set(SharedTokenHeader, fakeToken)
		p.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusMethodNotAllowed {
			t.Fatalf("POST cache endpoint status = %d, want %d", rec2.Code, http.StatusMethodNotAllowed)
		}
	})

	// 配置 token + 错误 header：直接 401，不管什么路径
	t.Run("wrong_token_header_rejects_with_401", func(t *testing.T) {
		p := NewHTTPPool("http://self")
		p.SetSharedToken(fakeToken)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, defaultBasePath+"g/k", nil)
		req.Header.Set(SharedTokenHeader, "wrong-token")
		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("wrong token status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})
}

// 回归 #22：token 校验改为恒定时间比较后，语义必须保持不变，
// 包括前缀相同、长度不同、空 header 等边界。
func TestAuthorizedComparesTokenCorrectly(t *testing.T) {
	p := NewHTTPPool("http://self")

	// 未配置 token 时不鉴权。
	if !p.authorized("") || !p.authorized("anything") {
		t.Fatal("empty token config must disable auth")
	}

	const token = "super-secret"
	p.SetSharedToken(token)

	tests := []struct {
		name string
		got  string
		want bool
	}{
		{"exact match", token, true},
		{"last byte differs", "super-secreT", false},
		{"prefix only", "super-secre", false},
		{"longer than token", token + "-longer", false},
		{"empty header", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := p.authorized(tt.got); got != tt.want {
				t.Fatalf("authorized(%q) = %v, want %v", tt.got, got, tt.want)
			}
		})
	}
}

// 回归测试 #12：/health 和 /metrics 仍按原语义处理，不受方法检查影响。
func TestServeHTTPHealthAndMetricsUnaffected(t *testing.T) {
	p := NewHTTPPool("http://self")

	t.Run("health_returns_200", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("/health status = %d, want %d", rec.Code, http.StatusOK)
		}
		if rec.Body.String() != "OK" {
			t.Fatalf("/health body = %q, want %q", rec.Body.String(), "OK")
		}
	})

	t.Run("metrics_returns_200", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		p.ServeHTTP(rec, req)

		// MetricsHandler 只要不 panic、返回 200 就算正常
		if rec.Code != http.StatusOK {
			t.Fatalf("/metrics status = %d, want %d", rec.Code, http.StatusOK)
		}
	})
}
