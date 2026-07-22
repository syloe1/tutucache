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
}

func (c *cache) add(key string, value ByteView, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, value, ttl)
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
		return v.(ByteView), ok

	}
	return
}

// StartCleanup 启动后台过期清理 goroutine，每 interval 扫描一次
// 调用方应在创建 cache 后调用此方法（通常由 Group 触发）
func (c *cache) StartCleanup(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			c.mu.Lock()
			n := c.lru.CleanupExpired()
			c.mu.Unlock()
			if n > 0 {
				log.Printf("[cache] cleaned up %d expired entries", n)
			}
		}
	}()
}
