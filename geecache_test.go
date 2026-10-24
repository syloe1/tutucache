package geecache

import (
	"errors"
	"fmt"
	pb "geecache/geecachepb"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubPeerGetter 模拟 peer，返回指定错误并记录调用次数。
type stubPeerGetter struct {
	err   error
	calls atomic.Int32
}

func (s *stubPeerGetter) Get(*pb.Request, *pb.Response) error {
	s.calls.Add(1)
	return s.err
}

func (s *stubPeerGetter) String() string { return "stub-peer" }

// 固定返回指定peer
type stubPicker struct {
	g PeerGetter
}

func (s stubPicker) PickPeer(string) (PeerGetter, bool) {
	return s.g, s.g != nil
}

// TestHTTPGetterReturnsPeerHTTPError 验证非 200 响应映射为带状态码的类型化错误。
func TestHTTPGetterReturnsPeerHTTPError(t *testing.T) {
	tests := []struct {
		name     string
		code     int
		isConfig bool
	}{
		{"401 Unauthorized", http.StatusUnauthorized, true},
		{"403 Forbidden", http.StatusForbidden, true},
		{"404 Not Found", http.StatusNotFound, true},
		{"500 Internal Server Error", http.StatusInternalServerError, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.code)
			}))
			defer srv.Close()

			getter := newHTTPGetter(srv.URL+defaultBasePath, nil, "")
			err := getter.Get(&pb.Request{Group: "g", Key: "k"}, &pb.Response{})

			var peerErr *PeerHTTPError
			if !errors.As(err, &peerErr) {
				t.Fatalf("error = %v, want *PeerHTTPError", err)
			}
			if peerErr.StatusCode != tt.code {
				t.Errorf("StatusCode = %d, want %d", peerErr.StatusCode, tt.code)
			}
			if got := peerErr.IsConfigError(); got != tt.isConfig {
				t.Errorf("IsConfigError() = %v, want %v", got, tt.isConfig)
			}
		})
	}
}

// TestHTTPGetterTransportErrorIsNotPeerHTTPError 网络错误不能被误分类为 PeerHTTPError，
// 否则会被当成配置错误而不是降级本地。
func TestHTTPGetterTransportErrorIsNotPeerHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL + defaultBasePath
	srv.Close() // 连接被拒绝

	err := newHTTPGetter(url, nil, "").Get(&pb.Request{Group: "g", Key: "k"}, &pb.Response{})
	if err == nil {
		t.Fatal("expected transport error")
	}
	var peerErr *PeerHTTPError
	if errors.As(err, &peerErr) {
		t.Fatalf("transport error misclassified as PeerHTTPError: %v", err)
	}
}

// TestServeHTTPHidesInternalError 验证 500 响应不泄露内部错误文本。
func TestServeHTTPHidesInternalError(t *testing.T) {
	const (
		secret = "db-dsn=postgres://internal-secret"
		group  = "test-http-hide-internal-error"
	)
	NewGroup(group, 1<<10, GetterFunc(func(string) ([]byte, error) {
		return nil, errors.New(secret)
	}))
	srv := httptest.NewServer(NewHTTPPool("http://self"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + defaultBasePath + group + "/k")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	if strings.Contains(string(body), secret) {
		t.Fatalf("response leaked internal error: %q", body)
	}
	if got := strings.TrimSpace(string(body)); got != "internal error" {
		t.Fatalf("body = %q, want %q", got, "internal error")
	}
}

// TestLoadSurfacesConfigError 配置错误(401/403/404)不会降级本地，不会调用本地 getter。
func TestLoadSurfacesConfigError(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var localCalls atomic.Int32
			g := NewGroup(fmt.Sprintf("test_config_err_%d", code), 1<<10, GetterFunc(func(string) ([]byte, error) {
				localCalls.Add(1)
				return nil, errors.New("local")
			}))
			g.RegisterPeers(&stubPicker{g: &stubPeerGetter{err: &PeerHTTPError{StatusCode: code}}})

			_, err := g.Get("key")
			if !isPeerConfigError(err) {
				t.Fatalf("expect config error, got %v", err)
			}
			if localCalls.Load() != 0 {
				t.Error("local getter was called, config error should NOT fallback local")
			}
		})
	}
}

// TestLoadFallsBackOnServerError 5xx 临时故障会降级本地。
func TestLoadFallsBackOnServerError(t *testing.T) {
	var localCalls atomic.Int32
	g := NewGroup("test_5xx", 1<<10, GetterFunc(func(string) ([]byte, error) {
		localCalls.Add(1)
		return []byte("local_data"), nil
	}))
	g.RegisterPeers(&stubPicker{g: &stubPeerGetter{
		err: &PeerHTTPError{StatusCode: http.StatusInternalServerError},
	}})

	val, err := g.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if string(val.ByteSlice()) != "local_data" {
		t.Error("should load from local")
	}
	if localCalls.Load() != 1 {
		t.Errorf("local getter calls = %d, want 1", localCalls.Load())
	}
}

// valuePeerGetter 返回预置字节切片，便于观察 ByteView 是否与响应缓冲区共享底层数组。
type valuePeerGetter struct {
	value []byte
	calls atomic.Int32
}

func (p *valuePeerGetter) Get(_ *pb.Request, out *pb.Response) error {
	p.calls.Add(1)
	out.Value = p.value
	return nil
}

// 回归 #16：peer 返回的 ByteView 必须独占底层数组，不能被响应缓冲区后续修改影响。
func TestGetFromPeerClonesValue(t *testing.T) {
	g := NewGroup("test_peer_clone", 1<<10, GetterFunc(func(string) ([]byte, error) {
		t.Fatal("local getter must not be called")
		return nil, nil
	}))

	buf := []byte("peer-data")
	peer := &valuePeerGetter{value: buf}

	v, err := g.getFromPeer(peer, "key")
	if err != nil {
		t.Fatalf("getFromPeer: %v", err)
	}
	if got := v.String(); got != "peer-data" {
		t.Fatalf("value = %q, want %q", got, "peer-data")
	}

	// 模拟对端响应缓冲区被复用/改写：ByteView 必须是独立副本。
	buf[0] = 'X'
	if got := v.String(); got != "peer-data" {
		t.Fatalf("peer value aliases response buffer: got %q", got)
	}
}

// 回归 #15：peer 命中的值必须回填本地缓存，第二次读取不应再次访问远端。
func TestPeerLoadPopulatesLocalCache(t *testing.T) {
	const name = "test_peer_cache"
	var localCalls atomic.Int32
	peer := &valuePeerGetter{value: []byte("peer-data")}

	g := NewGroup(name, 1<<10, GetterFunc(func(string) ([]byte, error) {
		localCalls.Add(1)
		return []byte("local-data"), nil
	}))
	t.Cleanup(func() {
		mu.Lock()
		delete(groups, name)
		mu.Unlock()
	})
	g.RegisterPeers(stubPicker{g: peer})

	for i := 0; i < 2; i++ {
		v, err := g.Get("key")
		if err != nil {
			t.Fatalf("Get #%d: %v", i+1, err)
		}
		if got := v.String(); got != "peer-data" {
			t.Fatalf("Get #%d value = %q, want %q", i+1, got, "peer-data")
		}
	}

	if got := peer.calls.Load(); got != 1 {
		t.Fatalf("peer calls = %d, want 1: second read should hit local cache", got)
	}
	if got := localCalls.Load(); got != 0 {
		t.Fatalf("local getter calls = %d, want 0", got)
	}
}

// TestConfigErrorDoesNotTripBreaker 连续多次配置错误不会打开熔断器。
func TestConfigErrorDoesNotTripBreaker(t *testing.T) {
	peer := &stubPeerGetter{err: &PeerHTTPError{StatusCode: http.StatusUnauthorized}}
	g := NewGroup("test_breaker", 1<<10, GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))
	g.RegisterPeers(&stubPicker{g: peer})

	for i := 0; i < 4; i++ {
		if _, err := g.Get(fmt.Sprintf("key-%d", i)); !isPeerConfigError(err) {
			t.Fatalf("iteration %d: expect config error, got %v", i, err)
		}
	}
	if got := peer.calls.Load(); got != 4 {
		t.Fatalf("peer calls = %d, want 4 (breaker must stay closed)", got)
	}
}

// TestConfigErrorReleasesHalfOpenProbe 验证配置错误会释放 HalfOpen 的唯一探测名额。
// 如果不释放，熔断器会永久卡在半开状态，之后的请求全部静默降级。
func TestConfigErrorReleasesHalfOpenProbe(t *testing.T) {
	peer := &stubPeerGetter{err: &PeerHTTPError{StatusCode: http.StatusInternalServerError}}
	g := NewGroup("test_half_open", 1<<10, GetterFunc(func(string) ([]byte, error) {
		return []byte("local"), nil
	}))
	g.RegisterPeers(&stubPicker{g: peer})
	g.breaker = NewCircuitBreaker(1, time.Millisecond)

	// 第一次 5xx → 熔断器打开。
	if _, err := g.Get("k1"); err != nil {
		t.Fatalf("5xx should fall back to local: %v", err)
	}

	time.Sleep(5 * time.Millisecond) // 等待冷却期结束，进入 HalfOpen

	// 半开探测拿到 401（配置错误）：错误必须暴露，探测名额必须释放。
	peer.err = &PeerHTTPError{StatusCode: http.StatusUnauthorized}
	if _, err := g.Get("k2"); !isPeerConfigError(err) {
		t.Fatalf("want config error from half-open probe, got %v", err)
	}

	// 第三次请求必须能再次访问 peer；否则说明探测名额泄漏，被 ErrCircuitOpen 短路。
	peer.err = nil
	if _, err := g.Get("k3"); err != nil {
		t.Fatalf("breaker stuck after config-error probe: %v", err)
	}
	if got := peer.calls.Load(); got != 3 {
		t.Fatalf("peer calls = %d, want 3 (half-open probe slot leaked)", got)
	}
}

func TestNewGroupRejectsNegativeCacheBytes(t *testing.T) {
	const name = "negative-cache-bytes"
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewGroup with negative cacheBytes should panic")
		}
		if GetGroup(name) != nil {
			t.Fatal("group with invalid cacheBytes must not be registered")
		}
	}()
	NewGroup(name, -1, GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	}))
}

func TestNewGroupZeroCacheBytesIsUnlimited(t *testing.T) {
	g := NewGroup("unlimited-cache-bytes", 0, GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	}))
	if _, err := g.Get("a"); err != nil {
		t.Fatalf("Get on unlimited group failed: %v", err)
	}
	if got := g.mainCache.lru.Len(); got != 1 {
		t.Fatalf("len=%d, want 1 (0 means unlimited)", got)
	}
}

func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get("key"); !reflect.DeepEqual(v, expect) {
		t.Errorf("callback failed")
	}

}

func TestMetrics(t *testing.T) {
	// 创建很小的缓存（10 字节），确保写入几个 key 后就触发淘汰
	g := NewGroup("m", 10, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("hello-" + key), nil // 每个 value 约 7-8 字节
		},
	))

	// 第一次 Get("a"): miss → local load → 缓存 a（key 1B + value 7B = 8B）
	g.Get("a")
	// 第二次 Get("a"): hit
	g.Get("a")
	// Get("b"): miss → local load → 写入 b（总 16B > 10B → 淘汰 a）
	g.Get("b")
	// Get("c"): miss → local load → 写入 c（再淘汰一个）
	g.Get("c")

	s := g.Metrics()
	if s.Hits != 1 {
		t.Fatalf("hits=%d, want 1", s.Hits)
	}
	if s.Misses != 3 {
		t.Fatalf("misses=%d, want 3", s.Misses)
	}
	if s.LocalLoads != 3 {
		t.Fatalf("local loads=%d, want 3", s.LocalLoads)
	}
	if s.Evictions != 2 {
		t.Fatalf("evictions=%d, want 2", s.Evictions)
	}
	rate := g.metrics.HitRate()
	if rate != 0.25 {
		t.Fatalf("lifetime hit rate=%.4f, want 0.25", rate)
	}
	t.Logf("hits=%d misses=%d loads=%d evictions=%d hit_rate=%.2f",
		s.Hits, s.Misses, s.LocalLoads, s.Evictions, rate)
}

func TestMetricsPerGroup(t *testing.T) {
	getter := GetterFunc(func(key string) ([]byte, error) {
		return []byte("v-" + key), nil
	})
	a := NewGroup("metrics-group-a", 1<<20, getter)
	b := NewGroup("metrics-group-b", 1<<20, getter)

	a.Get("k") // miss + local load
	a.Get("k") // hit

	as, bs := a.Metrics(), b.Metrics()
	if as.Hits != 1 || as.Misses != 1 || as.LocalLoads != 1 {
		t.Fatalf("group a metrics changed: %+v", as)
	}
	if bs != (MetricsSnapshot{}) {
		t.Fatalf("group b must stay isolated, got %+v", bs)
	}
	if a.metrics == b.metrics {
		t.Fatal("groups must not share a Metrics instance")
	}
}

func TestCacheBytes(t *testing.T) {
	g := NewGroup("cache-bytes", 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("v-" + key), nil
		},
	))

	if got := g.CacheBytes(); got != 0 {
		t.Fatalf("empty group bytes=%d, want 0", got)
	}

	g.Get("a") // key "a" (1B) + value "v-a" (3B) = 4B
	if got := g.CacheBytes(); got != 4 {
		t.Fatalf("bytes after populate=%d, want 4", got)
	}

	g.Get("a") // 命中不应改变占用
	if got := g.CacheBytes(); got != 4 {
		t.Fatalf("bytes after hit=%d, want 4", got)
	}
}

func TestMetricsHandlerCacheBytes(t *testing.T) {
	getter := GetterFunc(func(key string) ([]byte, error) {
		return []byte("v-" + key), nil
	})
	a := NewGroup("metrics-bytes-a", 1<<20, getter)
	NewGroup("metrics-bytes-b", 1<<20, getter) // 不写入，导出值应为 0

	a.Get("x") // key "x" (1B) + value "v-x" (3B) = 4B

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rec, req)

	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE geecache_cache_bytes gauge",
		`geecache_cache_bytes{group="metrics-bytes-a"} 4`,
		`geecache_cache_bytes{group="metrics-bytes-b"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestMetricsHandlerPerGroupLabels(t *testing.T) {
	getter := GetterFunc(func(key string) ([]byte, error) {
		return []byte("v-" + key), nil
	})
	a := NewGroup("metrics-handler-a", 1<<20, getter)
	b := NewGroup("metrics-handler-b", 1<<20, getter)

	a.Get("x") // miss
	a.Get("x") // hit
	b.Get("y") // miss

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("Content-Type=%q, want text/plain", ct)
	}

	body := rec.Body.String()
	for _, want := range []string{
		`geecache_hits_total{group="metrics-handler-a"} 1`,
		`geecache_misses_total{group="metrics-handler-a"} 1`,
		`geecache_local_loads_total{group="metrics-handler-a"} 1`,
		`geecache_hits_total{group="metrics-handler-b"} 0`,
		`geecache_misses_total{group="metrics-handler-b"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}

	if strings.Contains(body, "geecache_hit_rate") {
		t.Fatal("lifetime hit-rate gauge must not be exported")
	}
	if n := strings.Count(body, "# TYPE geecache_hits_total counter"); n != 1 {
		t.Fatalf("HELP/TYPE for hits emitted %d times, want 1", n)
	}
}

func TestMetricsConcurrentRecording(t *testing.T) {
	const (
		workers   = 8
		perWorker = 1000
	)
	m := NewMetrics()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				m.RecordHit()
				m.RecordMiss()
				_ = m.Snapshot()
			}
		}()
	}
	wg.Wait()

	s := m.Snapshot()
	if s.Hits != workers*perWorker || s.Misses != workers*perWorker {
		t.Fatalf("snapshot=%+v, want hits=misses=%d", s, workers*perWorker)
	}
}

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

// TestHTTPProtoRoundTrip 走完整的 HTTP 传输路径（服务端 Marshal → 客户端 Unmarshal），
// 确保 http.go 与 peers_grpc.go 使用同一个 protobuf runtime 后线格式仍然一致。
func TestHTTPProtoRoundTrip(t *testing.T) {
	const group = "http-proto-roundtrip"
	NewGroup(group, 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("v-" + key), nil
		},
	))

	pool := NewHTTPPool("http://self")
	srv := httptest.NewServer(pool)
	defer srv.Close()

	getter := newHTTPGetter(srv.URL+defaultBasePath, nil, "")
	res := &pb.Response{}
	if err := getter.Get(&pb.Request{Group: group, Key: "k"}, res); err != nil {
		t.Fatalf("HTTP get failed: %v", err)
	}
	if got := string(res.Value); got != "v-k" {
		t.Fatalf("value=%q, want %q", got, "v-k")
	}
}

func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := NewGroup("scores-lru", 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key] += 1
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		if view, err := gee.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		} // load from callback function
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		} // cache hit
	}

	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view)
	}
}

// 回归测试 #29：Getter 在加载 K 时递归调用 Get(K) 不能死锁，
// 应立即返回 ErrReentrantLoad，并让外层调用正常结束。
func TestReentrantGetterFailsFast(t *testing.T) {
	var g *Group
	g = NewGroup("reentrant-getter", 2<<10, GetterFunc(func(k string) ([]byte, error) {
		if k == "K" {
			v, err := g.Get("K")
			if err != nil {
				return nil, err
			}
			return v.ByteSlice(), nil
		}
		return []byte("v"), nil
	}))

	done := make(chan error, 1)
	go func() {
		_, err := g.Get("K")
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrReentrantLoad) {
			t.Fatalf("Get(K) error = %v, want ErrReentrantLoad", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("re-entrant Getter deadlocked on Get(K)")
	}
}

// Getter 递归加载不同的 key 是合法用法，不能被 re-entrant 检测误伤。
func TestGetterCanLoadOtherKey(t *testing.T) {
	var g *Group
	g = NewGroup("nested-other-key", 2<<10, GetterFunc(func(k string) ([]byte, error) {
		if k == "A" {
			v, err := g.Get("B")
			if err != nil {
				return nil, err
			}
			return append([]byte("A:"), v.ByteSlice()...), nil
		}
		return []byte("B"), nil
	}))

	v, err := g.Get("A")
	if err != nil {
		t.Fatalf("Get(A) error = %v, want nil", err)
	}
	if got := v.String(); got != "A:B" {
		t.Fatalf("Get(A) = %q, want %q", got, "A:B")
	}
}

// TestGroupRegisterPeersConcurrentWithLoad 回归 #5：
// RegisterPeers 的写与 load 的读之间必须存在 happens-before。
// 数据竞争只在 go test -race 下能被捕获。
func TestGroupRegisterPeersConcurrentWithLoad(t *testing.T) {
	const (
		readers       = 8
		keysPerReader = 500
	)
	g := NewGroup("register-peers-race", 1<<20, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))
	picker := stubPicker{g: &stubPeerGetter{}}

	start := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		g.RegisterPeers(picker)
	}()

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for j := 0; j < keysPerReader; j++ {
				if _, err := g.Get(fmt.Sprintf("race-%d-%d", id, j)); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()
}

// TestGroupRegisterPeersConcurrentDuplicatePanicsOnce 回归 #5 的非原子重复注册检查：
// 并发重复注册时恰好一次成功，其余全部 panic，不会出现静默覆盖。
func TestGroupRegisterPeersConcurrentDuplicatePanicsOnce(t *testing.T) {
	const workers = 16
	g := NewGroup("register-peers-duplicate", 1<<20, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))

	start := make(chan struct{})
	var (
		ok       atomic.Int32
		panicked atomic.Int32
		wg       sync.WaitGroup
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					panicked.Add(1)
				}
			}()
			<-start
			g.RegisterPeers(stubPicker{g: &stubPeerGetter{}})
			ok.Add(1)
		}()
	}
	close(start)
	wg.Wait()

	if got := ok.Load(); got != 1 {
		t.Fatalf("successful registrations = %d, want 1", got)
	}
	if got := panicked.Load(); got != workers-1 {
		t.Fatalf("panics = %d, want %d", got, workers-1)
	}
}

// TestGroupSetTTLConcurrentWithGet 回归 #6：SetTTL 与 cache miss 触发的 populateCache 无竞争
func TestGroupSetTTLConcurrentWithGet(t *testing.T) {
	g := NewGroup("set-ttl-race", 1<<20, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))

	start := make(chan struct{})
	var wg sync.WaitGroup

	// 写端：并发修改 TTL
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			g.SetTTL(time.Duration(i)*time.Second, 0)
		}
	}()

	// 读端：并发 Get 触发 cache miss + populateCache
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for j := 0; j < 200; j++ {
				key := fmt.Sprintf("ttl-race-%d-%d", id, j)
				if _, err := g.Get(key); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()
}

// TestGroupTTL 验证默认 TTL 的读写访问器：
// 初始值为 0（永不过期），SetTTL 后 TTL 返回新值。
func TestGroupTTL(t *testing.T) {
	g := NewGroup("ttl-accessor", 1<<20, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))
	if got := g.TTL(); got != 0 {
		t.Fatalf("initial TTL = %v, want 0", got)
	}
	g.SetTTL(2*time.Minute, 0)
	if got := g.TTL(); got != 2*time.Minute {
		t.Fatalf("TTL after SetTTL = %v, want 2m", got)
	}
}

// TestGroupPurgeBeforeAnyAdd 空缓存调用Purge不能panic
func TestGroupPurgeBeforeAnyAdd(t *testing.T) {
	g := NewGroup("purge-empty", 1<<10, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))
	g.Purge() // c.lru 尚未初始化，不能 panic

	if got := g.CacheBytes(); got != 0 {
		t.Fatalf("CacheBytes after empty Purge = %d, want 0", got)
	}
}

// TestGroupSetTTLIsForwardLookingAndPurge 钉死前瞻语义
func TestGroupSetTTLIsForwardLookingAndPurge(t *testing.T) {
	g := NewGroup("ttl-forward-looking", 1<<20, GetterFunc(func(k string) ([]byte, error) {
		return []byte("v-" + k), nil
	}))

	// 预热：旧条目在 defaultTTL == 0 时写入，永不过期。
	if _, err := g.Get("old"); err != nil {
		t.Fatal(err)
	}

	g.SetTTL(30*time.Millisecond, 0)
	if _, err := g.Get("new"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if _, ok := g.mainCache.get("old"); !ok {
		t.Fatal("old entry should keep its original zero expiry (forward-looking semantics)")
	}
	if _, ok := g.mainCache.get("new"); ok {
		t.Fatal("new entry should expire under the new TTL")
	}

	// 先 SetTTL 再 Purge：重新加载的 old 应该使用新 TTL。
	g.SetTTL(30*time.Millisecond, 0)
	g.Purge()
	if _, err := g.Get("old"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, ok := g.mainCache.get("old"); ok {
		t.Fatal("reloaded entry should expire under the new TTL after Purge")
	}
}

// 回归 #31：SetTTL(ttl, 0) 不启动后台清理时，过期冷 key 仍会随后续写入被回收，
// 不会永久占用 LRU 槽位。
func TestGroupExpiredColdKeyReclaimedWithoutCleanup(t *testing.T) {
	const name = "expired-cold-no-cleanup"
	g := NewGroup(name, 1<<20, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))
	t.Cleanup(func() {
		mu.Lock()
		delete(groups, name)
		mu.Unlock()
	})

	g.SetTTL(10*time.Millisecond, 0) // 明确不启动后台清理
	if _, err := g.Get("cold"); err != nil {
		t.Fatalf("Get(cold): %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// 写入 "other" 会顺带摊销扫描冷端，删除已经过期的 "cold"。
	if _, err := g.Get("other"); err != nil {
		t.Fatalf("Get(other): %v", err)
	}

	g.mainCache.mu.Lock()
	n := g.mainCache.lru.Len()
	g.mainCache.mu.Unlock()
	if n != 1 {
		t.Fatalf("lru len=%d, want 1 (expired cold key should not stay resident)", n)
	}
}

// TestGroupPurgeConcurrentWithGet -race 下无数据竞争
func TestGroupPurgeConcurrentWithGet(t *testing.T) {
	g := NewGroup("purge-race", 1<<20, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, err := g.Get(fmt.Sprintf("purge-%d-%d", id, j)); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			g.Purge()
		}
	}()
	wg.Wait()
}

// 回归 #28：同名 Group 必须 fail fast，且不能覆盖已经注册的实例。
func TestNewGroupDuplicateNamePanics(t *testing.T) {
	const name = "dup-test"
	g1 := NewGroup(name, 1024, GetterFunc(func(string) ([]byte, error) { return nil, nil }))
	// 测试自己占用了全局注册表里的名字，跑完必须清理，保证 -count 重跑不受影响。
	t.Cleanup(func() {
		mu.Lock()
		delete(groups, name)
		mu.Unlock()
	})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewGroup with a duplicate name did not panic")
		}
		want := `geecache: NewGroup called twice with the same name: "dup-test"`
		if got, ok := r.(string); !ok || got != want {
			t.Fatalf("panic = %v (%T), want %q", r, r, want)
		}
		if got := GetGroup(name); got != g1 {
			t.Fatal("duplicate NewGroup replaced the existing group")
		}
	}()

	NewGroup(name, 1024, GetterFunc(func(string) ([]byte, error) { return nil, nil }))
}
