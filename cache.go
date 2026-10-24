package geecache

import (
	"geecache/lru"
	"log"
	"sync"
	"time"
)

// cleanupBatchSize 是后台过期清理每批持锁扫描的节点数。
// 批与批之间会释放 c.mu，避免一次 O(n) 全量扫描长时间阻塞 get/add。
const cleanupBatchSize = 1024

// addExpiryScanLimit 是每次写入时顺带检查的冷端节点数。
// 即使调用方没有启动后台清理循环（cleanupInterval == 0），写入路径也会以
// 摊销方式回收过期冷 key：它们不会永久占住 LRU 槽位，最多在后续有限次写入
// 后全部被清理。没有新写入时缓存本来就不会继续增长，因此不需要额外 goroutine。
const addExpiryScanLimit = 2

// 实例化LRU, 封装get add
type cache struct {
	mu         sync.Mutex
	lru        *lru.Cache
	cacheBytes int64

	// onEvicted 在 c.mu 释放之后调用，参数是刚被容量淘汰或过期删除的条目。
	// 生产环境用它记录淘汰指标；测试可以注入会重入缓存的回调，验证不会死锁。
	// 回调可能被多个 goroutine 并发调用，也可能与后续的 get/add 交错执行，
	// 因此实现必须并发安全，且不能假设缓存仍处于淘汰发生时的状态。
	// 必须在 cache 首次使用前设置，之后只读。
	onEvicted func(item lru.EvictItem)

	// cleanupMu 只保护下面清理循环的生命周期字段。
	// 不复用 mu：StopCleanup 需要等待清理 goroutine 退出，而该 goroutine
	// 可能正持有 mu 执行清理，复用会死锁。
	cleanupMu     sync.Mutex
	cleanupStop   chan struct{}
	cleanupDone   chan struct{}
	cleanupClosed bool
}

// purge 清空本地 LRU；尚未初始化时直接返回。
func (c *cache) purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.Purge()
}

func (c *cache) add(key string, value ByteView, ttl time.Duration) {
	var evicted []lru.EvictItem
	func() {
		c.mu.Lock()
		// 用 defer 保证 lru.Add 即使 panic（例如 Value.Len 实现有问题）也会释放锁。
		defer c.mu.Unlock()
		if c.lru == nil {
			// 懒初始化，第一次add才创建lru
			c.lru = lru.New(c.cacheBytes)
		}
		evicted = c.lru.Add(key, value, ttl)
		// 借写入路径摊销回收过期冷 key。没有后台清理循环时，这是唯一的
		// 主动回收入口；摊销游标与后台全量清理相互独立。
		evicted = append(evicted, c.lru.CleanupExpiredAmortized(addExpiryScanLimit)...)
	}()
	// 锁已释放，再执行淘汰回调。
	c.handleEvictions(evicted)
}

// Bytes 返回本地缓存当前的字节占用；LRU 尚未初始化时为 0。
// 直接读取 LRU 的实时状态，不额外维护计数器，避免两者漂移。
func (c *cache) Bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return 0
	}
	return c.lru.Bytes()
}

/*
LRU的get会执行MoveTofront修改链表结构
*/
func (c *cache) get(key string) (value ByteView, ok bool) {
	var (
		v       lru.Value
		evicted []lru.EvictItem
	)
	func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.lru == nil {
			return
		}
		v, ok, evicted = c.lru.Get(key)
	}()

	// 锁已释放，再执行惰性过期回调。
	c.handleEvictions(evicted)

	if !ok {
		return ByteView{}, false
	}
	// lru 中只保存 ByteView，这里的类型断言是安全的。
	return v.(ByteView), true
}

// handleEvictions 必须在 c.mu 已释放的前提下调用。
// 回调可能重入 cache 或执行慢速 I/O，放在锁外可以避免死锁和阻塞缓存读写。
func (c *cache) handleEvictions(items []lru.EvictItem) {
	if c.onEvicted == nil {
		return
	}
	for _, item := range items {
		c.onEvicted(item)
	}
}

// StartCleanup 启动后台过期清理 goroutine，每 interval 扫描一次。
// 每个 cache 至多只有一个清理循环：重复调用是幂等的，首次的非零 interval 生效。
// 通常由 Group.SetTTL 触发；调用 StopCleanup 可停止循环。
func (c *cache) StartCleanup(interval time.Duration) {
	if interval <= 0 {
		// time.NewTicker 会对非正周期 panic，这里直接忽略。
		return
	}

	c.cleanupMu.Lock()
	// 已经启动过，或已被 StopCleanup 关闭，都不再启动新的循环。
	if c.cleanupStop != nil || c.cleanupClosed {
		c.cleanupMu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	c.cleanupStop = stop
	c.cleanupDone = done
	c.cleanupMu.Unlock()

	go c.cleanupLoop(interval, stop, done)
}

func (c *cache) cleanupLoop(interval time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			c.cleanupExpired(stop)
		}
	}
}

// cleanupExpired 分批删除所有已过期条目。
// 每批只持锁扫描 cleanupBatchSize 个节点，批间释放 c.mu，
// 因此大缓存的一次清理不会把 get/add 阻塞整个扫描时长。
// 一轮扫描的总工作量仍是 O(n)，只是把临界区切成了有界的多段。
func (c *cache) cleanupExpired(stop <-chan struct{}) {
	total := 0
	defer func() {
		if total > 0 {
			log.Printf("[cache] cleaned up %d expired entries", total)
		}
	}()

	for {
		evicted, finished := c.cleanupBatch()
		total += len(evicted)
		// 锁已释放，再执行过期回调。
		c.handleEvictions(evicted)
		if finished {
			return
		}
		// StopCleanup 可能在批次之间到达；最多处理完当前批次就退出。
		select {
		case <-stop:
			return
		default:
		}
	}
}

// cleanupBatch 持锁执行一批过期扫描，返回被删除的条目以及本轮扫描是否结束。
func (c *cache) cleanupBatch() (evicted []lru.EvictItem, finished bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return nil, true
	}
	return c.lru.CleanupExpiredBatch(cleanupBatchSize)
}

// StopCleanup 停止后台清理循环并等待其退出；可重复调用。
// 停止后该 cache 不会再启动新的清理循环。
func (c *cache) StopCleanup() {
	c.cleanupMu.Lock()
	c.cleanupClosed = true
	stop, done := c.cleanupStop, c.cleanupDone
	c.cleanupStop, c.cleanupDone = nil, nil
	c.cleanupMu.Unlock()

	if stop == nil {
		return
	}
	close(stop)
	// done 只由 cleanupLoop 关闭，等待它可以保证返回时 goroutine 已退出。
	<-done
}

//cc
