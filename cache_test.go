package geecache

import (
	"fmt"
	"geecache/lru"
	"sync"
	"testing"
	"time"
)

// cleanupChannels 返回 cache 当前清理循环的 stop/done 通道，供测试断言。
func cleanupChannels(c *cache) (stop, done chan struct{}) {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	return c.cleanupStop, c.cleanupDone
}

func TestStartCleanupIsIdempotent(t *testing.T) {
	c := &cache{}
	c.StartCleanup(time.Hour)

	stop, done := cleanupChannels(c)
	if stop == nil || done == nil {
		t.Fatal("cleanup loop did not start")
	}

	for i := 0; i < 100; i++ {
		c.StartCleanup(time.Hour)
	}

	stop2, done2 := cleanupChannels(c)
	if stop2 != stop || done2 != done {
		t.Fatal("repeated StartCleanup replaced the running cleanup loop")
	}

	c.StopCleanup()
	select {
	case <-done:
	default:
		t.Fatal("cleanup loop still running after StopCleanup")
	}
}

func TestStartCleanupIgnoresNonPositiveInterval(t *testing.T) {
	c := &cache{}
	c.StartCleanup(0)
	c.StartCleanup(-time.Second)

	if stop, _ := cleanupChannels(c); stop != nil {
		t.Fatal("cleanup loop started for a non-positive interval")
	}
}

func TestStopCleanupIsIdempotent(t *testing.T) {
	// 从未启动过清理循环时调用也必须是安全的。
	c := &cache{}
	c.StopCleanup()
	c.StopCleanup()

	if stop, _ := cleanupChannels(c); stop != nil {
		t.Fatal("StopCleanup started a cleanup loop")
	}

	// StopCleanup 之后不允许重新启动。
	c.StartCleanup(time.Hour)
	if stop, _ := cleanupChannels(c); stop != nil {
		t.Fatal("StartCleanup restarted a loop after StopCleanup")
	}

	// 已启动的循环可以重复 Stop，且第一次 Stop 返回时 goroutine 已退出。
	c2 := &cache{}
	c2.StartCleanup(time.Hour)
	_, done := cleanupChannels(c2)
	c2.StopCleanup()
	c2.StopCleanup()
	select {
	case <-done:
	default:
		t.Fatal("cleanup loop still running after StopCleanup")
	}
}

func TestCleanupLoopRemovesExpiredEntries(t *testing.T) {
	c := &cache{}
	c.add("k", ByteView{b: []byte("v")}, 10*time.Millisecond)
	c.StartCleanup(5 * time.Millisecond)
	defer c.StopCleanup()

	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := 0
		if c.lru != nil {
			n = c.lru.Len()
		}
		c.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired entry was not removed, lru len=%d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGroupCloseStopsCleanup(t *testing.T) {
	g := NewGroup("cleanup-group-close", 1<<10, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))

	g.SetTTL(time.Hour, time.Hour)
	stop, done := cleanupChannels(&g.mainCache)
	if stop == nil || done == nil {
		t.Fatal("SetTTL did not start the cleanup loop")
	}

	// 重复 SetTTL 只更新 TTL，不会额外启动清理循环。
	for i := 0; i < 50; i++ {
		g.SetTTL(time.Hour, time.Hour)
	}
	stop2, done2 := cleanupChannels(&g.mainCache)
	if stop2 != stop || done2 != done {
		t.Fatal("repeated SetTTL started additional cleanup loops")
	}

	g.Close()
	g.Close() // 幂等

	select {
	case <-done:
	default:
		t.Fatal("Group.Close did not stop the cleanup loop")
	}

	// Close 只停止后台清理，不影响正常读取。
	if _, err := g.Get("a"); err != nil {
		t.Fatalf("Get after Close failed: %v", err)
	}
}

// reentrantOnEvicted 返回一个会重入 cache 的淘汰回调。
// 如果回调在持有 c.mu 时执行，调用方会死锁。
func reentrantOnEvicted(c *cache, ran chan<- struct{}) func(lru.EvictItem) {
	var once sync.Once
	return func(lru.EvictItem) {
		c.get("missing")
		once.Do(func() { close(ran) })
	}
}

func waitForCallback(t *testing.T, ran <-chan struct{}) {
	t.Helper()
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("eviction callback ran while holding the cache lock")
	}
}

func TestEvictionCallbackRunsOutsideLockOnAdd(t *testing.T) {
	c := &cache{cacheBytes: 4}
	ran := make(chan struct{})
	c.onEvicted = reentrantOnEvicted(c, ran)

	c.add("a", ByteView{b: []byte("aa")}, 0) // 1+2=3 字节
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.add("b", ByteView{b: []byte("bb")}, 0) // 3+3 > 4，淘汰 a
	}()

	waitForCallback(t, ran)
	<-done
}

func TestEvictionCallbackRunsOutsideLockOnLazyExpiry(t *testing.T) {
	c := &cache{cacheBytes: 1000}
	ran := make(chan struct{})
	c.onEvicted = reentrantOnEvicted(c, ran)

	c.add("k", ByteView{b: []byte("v")}, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok := c.get("k"); ok {
			t.Error("expired key reported as a hit")
		}
	}()

	waitForCallback(t, ran)
	<-done
}

func TestEvictionCallbackRunsOutsideLockOnCleanup(t *testing.T) {
	c := &cache{cacheBytes: 1000}
	ran := make(chan struct{})
	c.onEvicted = reentrantOnEvicted(c, ran)

	c.add("k", ByteView{b: []byte("v")}, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.cleanupExpired(nil)
	}()

	waitForCallback(t, ran)
	<-done
}

func TestLazyExpiryRecordsEviction(t *testing.T) {
	m := NewMetrics()
	c := &cache{
		cacheBytes: 1000,
		onEvicted:  func(lru.EvictItem) { m.RecordEviction() },
	}
	c.add("k", ByteView{b: []byte("v")}, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if _, ok := c.get("k"); ok {
		t.Fatal("expired key reported as a hit")
	}
	if got := m.Snapshot().Evictions; got != 1 {
		t.Fatalf("evictions=%d, want 1", got)
	}
}

func TestCleanupRecordsEvictions(t *testing.T) {
	m := NewMetrics()
	c := &cache{
		cacheBytes: 1000,
		onEvicted:  func(lru.EvictItem) { m.RecordEviction() },
	}
	c.add("k", ByteView{b: []byte("v")}, 10*time.Millisecond)
	c.add("live", ByteView{b: []byte("v")}, 0)
	time.Sleep(20 * time.Millisecond)

	c.cleanupExpired(nil)

	if got := m.Snapshot().Evictions; got != 1 {
		t.Fatalf("evictions=%d, want 1", got)
	}
	// 只剩 "live"：key 4 字节 + value 1 字节。
	if got := c.Bytes(); got != 5 {
		t.Fatalf("cache bytes=%d, want 5", got)
	}
}

// 回归 #31：没有后台清理循环时，写入路径也要摊销回收过期冷 key。
func TestAddReclaimsExpiredColdKeysWithoutCleanup(t *testing.T) {
	m := NewMetrics()
	c := &cache{
		cacheBytes: 1 << 20,
		onEvicted:  func(lru.EvictItem) { m.RecordEviction() },
	}
	c.add("cold", ByteView{b: []byte("v")}, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	// 一次写入只能扫描有限的冷端节点，但足以回收这个唯一的过期条目。
	c.add("live", ByteView{b: []byte("v")}, 0)

	c.mu.Lock()
	n := c.lru.Len()
	c.mu.Unlock()
	if n != 1 {
		t.Fatalf("lru len=%d, want 1 (expired cold key should be reclaimed by Add)", n)
	}
	if got := m.Snapshot().Evictions; got != 1 {
		t.Fatalf("evictions=%d, want 1", got)
	}
}

func TestCleanupExpiredProcessesMultipleBatches(t *testing.T) {
	c := &cache{cacheBytes: 1 << 20}
	total := cleanupBatchSize*2 + 17
	for i := 0; i < total; i++ {
		c.add(fmt.Sprintf("k%d", i), ByteView{b: []byte("v")}, 10*time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	c.cleanupExpired(nil)

	if got := c.Bytes(); got != 0 {
		t.Fatalf("cache bytes after cleanup=%d, want 0", got)
	}
}

func TestEvictCallbackNoCacheMu(t *testing.T) {
	c := &cache{cacheBytes: 1024}
	checked := make(chan bool, 1)
	c.onEvicted = func(lru.EvictItem) {
		// 回调执行时 c.mu 必须可用。Go 的 Mutex 不可重入，
		// 如果回调是在持锁路径里调用的，这里 TryLock 会返回 false。
		ok := c.mu.TryLock()
		if ok {
			c.mu.Unlock()
		}
		select {
		case checked <- ok:
		default:
		}
	}

	// 每个条目 2 + 500 = 502 字节；第三个条目写入后超过 1024，触发淘汰。
	c.add("k1", ByteView{b: make([]byte, 500)}, 0)
	c.add("k2", ByteView{b: make([]byte, 500)}, 0)
	c.add("k3", ByteView{b: make([]byte, 500)}, 0)

	select {
	case ok := <-checked:
		if !ok {
			t.Fatal("cache.mu still held during eviction callback")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("eviction callback did not run")
	}
}
