package geecache

import (
	"geecache/lru"
	"log"
	"sync"
	"time"
)

// 实例化LRU, 封装get add
type cache struct {
	mu         sync.Mutex
	lru        *lru.Cache
	cacheBytes int64
	metrics    *Metrics

	// cleanupMu 只保护下面清理循环的生命周期字段。
	// 不复用 mu：StopCleanup 需要等待清理 goroutine 退出，而该 goroutine
	// 可能正持有 mu 执行 CleanupExpired，复用会死锁。
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		// 懒初始化，第一次add才创建lru
		c.lru = lru.New(c.cacheBytes, func(key string, v lru.Value) {
			if c.metrics != nil {
				c.metrics.RecordEviction()
			}
		})
	}
	c.lru.Add(key, value, ttl)
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		return
	}
	if v, ok := c.lru.Get(key); ok {
		// v.(ByteView)是类型断言
		return v.(ByteView), ok

	}
	return
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
			c.mu.Lock()
			if c.lru != nil {
				n := c.lru.CleanupExpired()
				if n > 0 {
					log.Printf("[cache] cleaned up %d expired entries", n)
				}
			}
			c.mu.Unlock()
		}
	}
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
