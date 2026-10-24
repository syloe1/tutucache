package lru

import (
	"container/list"
	"fmt"
	"time"
)

// Unlimited 表示不限制缓存内存占用，用于 New 的 maxBytes 参数。
// 0 是唯一表示"不限制"的取值；负数是非法配置，New 会在构造时 panic，
// 而不是静默地让每次 Add 都把刚写入的条目淘汰掉。
const Unlimited int64 = 0

// EvictItem 描述一个被容量淘汰或过期删除的条目。
//
// LRU 自身不执行任何回调：所有删除路径都把被删除的条目作为返回值交给调用方，
// 由调用方在释放自己的锁之后再处理。这样即使回调重入缓存也不会死锁，
// 慢回调也不会在持锁期间阻塞其他缓存操作。
type EvictItem struct {
	Key   string
	Value Value
}

// Cache 带TTL过期能力的LRU缓存
// 内存上限maxBytes；同时支持惰性过期(Get时检查) + 主动分批扫描过期(CleanupExpiredBatch)
//
// 并发：Cache 内部不加锁，调用方必须自行同步所有方法。
type Cache struct {
	maxBytes int64                    // 最大允许占用字节数，Unlimited(0)代表不限制内存
	nbytes   int64                    // 当前已经占用总内存(key+value)
	ll       *list.List               // 双向链表：Front是最近访问，Back是最久未访问
	cache    map[string]*list.Element // 哈希表，key映射链表节点，O(1)查找

	// cleanupCursor 是下一次全量清理 CleanupExpiredBatch 继续扫描的位置（向 Front 方向）。
	// amortizedCursor 是写入路径摊销清理 CleanupExpiredAmortized 的独立游标。
	// 两者必须分开：写入会持续小步推进，如果共用游标，后台的一轮全量扫描会误以为
	// 游标之前的节点“本轮已检查过”，从而漏掉扫描时尚未过期、之后才过期的条目。
	cleanupCursor   *list.Element
	amortizedCursor *list.Element
}

// entry 链表里面存储的单元，保存key、value、过期时间
type entry struct {
	key       string
	value     Value
	expiresAt time.Time // 过期时间；零值time.Time代表永不过期
}

// Value 接口，要求缓存值可以计算自身字节长度
type Value interface {
	Len() int
}

// New 创建LRU缓存实例
// maxBytes 必须 >= 0：Unlimited(0) 是显式的"不限制内存"哨兵值；
// 负数属于非法配置，构造时直接 panic，避免静默变成"每次 Add 立即淘汰"。
func New(maxBytes int64) *Cache {
	if maxBytes < 0 {
		panic(fmt.Sprintf("lru: maxBytes must be >= 0 (0 means unlimited), got %d", maxBytes))
	}
	return &Cache{
		maxBytes: maxBytes,
		ll:       list.New(),
		cache:    make(map[string]*list.Element),
	}
}

// Purge 清空所有缓存条目。调用方必须自行同步并发访问。
// 手动清理不属于容量淘汰，不会产生 EvictItem。
func (c *Cache) Purge() {
	c.ll.Init()
	c.cache = make(map[string]*list.Element)
	c.nbytes = 0
	// 链表已经重置，续扫游标必须失效；否则会继续遍历已脱离链表的旧节点。
	c.cleanupCursor = nil
	c.amortizedCursor = nil
}

// removeElement 从链表和 map 中删除节点，更新占用并维护续扫游标。
// 调用方必须持有保护 Cache 的锁。
func (c *Cache) removeElement(e *list.Element) *entry {
	kv := e.Value.(*entry)
	prev := e.Prev()
	c.ll.Remove(e)
	delete(c.cache, kv.key)
	c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
	if c.cleanupCursor == e {
		// 游标指向的节点被其他路径（惰性过期/容量淘汰）删除了，
		// 退回到前一个节点，下一次扫描从这里继续。
		c.cleanupCursor = prev
	}
	if c.amortizedCursor == e {
		c.amortizedCursor = prev
	}
	return kv
}

// Get 根据key读取缓存
// 惰性过期：命中时检查过期；已过期直接删除，返回ok=false，同时返回过期淘汰项。
// 淘汰项由调用方在释放锁后处理，LRU 内部不执行回调。
func (c *Cache) Get(key string) (value Value, ok bool, evicted []EvictItem) {
	ele, found := c.cache[key]
	if !found {
		return nil, false, nil
	}
	kv := ele.Value.(*entry)
	// 判断是否过期
	if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
		kv = c.removeElement(ele)
		return nil, false, []EvictItem{{Key: kv.key, Value: kv.value}}
	}
	// 命中未过期，移到链表头部（最近访问）
	c.ll.MoveToFront(ele)
	return kv.value, true, nil
}

// RemoveOldest 删除最久未访问节点（链表尾部），内存超限时调用。
// 返回被淘汰的条目；没有可删除节点时 ok=false。
func (c *Cache) RemoveOldest() (item EvictItem, ok bool) {
	ele := c.ll.Back()
	if ele == nil {
		return EvictItem{}, false
	}
	kv := c.removeElement(ele)
	return EvictItem{Key: kv.key, Value: kv.value}, true
}

// Add 添加/更新缓存key，支持TTL
// ttl>0 设置过期时间；ttl=0代表永不过期
// 添加完成后循环检查内存上限，超出则持续淘汰最旧节点
// 返回本次容量超限淘汰出来的所有条目
func (c *Cache) Add(key string, value Value, ttl time.Duration) []EvictItem {
	var evicted []EvictItem
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	if ele, ok := c.cache[key]; ok {
		// key已存在：更新value、过期时间，移动到头部
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		// 更新内存占用：新value大小减去旧value大小
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
		kv.expiresAt = expiresAt
	} else {
		// 新增key：插入链表头部，写入map，累加内存
		ele := c.ll.PushFront(&entry{key: key, value: value, expiresAt: expiresAt})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	// 内存超限，循环淘汰最久未访问
	for c.maxBytes != Unlimited && c.maxBytes < c.nbytes {
		if item, ok := c.RemoveOldest(); ok {
			evicted = append(evicted, item)
		}
	}
	return evicted
}

// Len 返回当前缓存条目数量
func (c *Cache) Len() int {
	return c.ll.Len()
}

// Bytes 返回当前缓存占用的字节数（所有条目的 key + value 之和）。
// 与 Len / CleanupExpired 一样，调用方必须自行同步对 Cache 的并发访问。
func (c *Cache) Bytes() int64 {
	return c.nbytes
}

// CleanupExpired 一次性扫描并删除全部已过期条目，返回被删除的条目。
// 这是一次 O(n) 操作，调用方会在整个扫描期间持有自己的锁；
// 需要控制单次持锁时间时，请改用 CleanupExpiredBatch。
func (c *Cache) CleanupExpired() []EvictItem {
	// 从尾部重新开始一轮完整扫描，丢弃之前未完成的分页进度。
	c.cleanupCursor = nil
	var evicted []EvictItem
	for {
		batch, finished := c.CleanupExpiredBatch(64)
		evicted = append(evicted, batch...)
		if finished {
			return evicted
		}
	}
}

// CleanupExpiredBatch 从上次扫描的位置继续向链表尾部（最冷端）扫描，
// 最多检查 limit 个节点（limit <= 0 时按 1 处理），删除其中已过期的条目。
// finished 为 true 表示本轮全量扫描结束；下一次调用会从尾部重新开始新的一轮。
//
// 分页的目的是限制单次持锁时间：调用方可以在每批之间释放锁，
// 让 Get/Add 在清理大缓存的间隙继续执行。
// 扫描期间被移动或新增的节点可能留到下一轮处理，不保证本轮一定覆盖。
// 与其他方法一样，调用方必须自行同步并发访问。
func (c *Cache) CleanupExpiredBatch(limit int) (evicted []EvictItem, finished bool) {
	return c.cleanupExpiredFrom(&c.cleanupCursor, limit)
}

// CleanupExpiredAmortized 从独立的摊销游标继续向链表头部方向扫描，
// 最多检查 limit 个节点并删除其中已过期的条目（limit <= 0 时按 1 处理）。
//
// 供写入路径调用，让没有后台清理循环的缓存也能逐步回收过期冷 key。
// 它不推进 CleanupExpiredBatch 的游标，因此不会破坏后台一轮全量扫描的覆盖范围。
func (c *Cache) CleanupExpiredAmortized(limit int) []EvictItem {
	evicted, _ := c.cleanupExpiredFrom(&c.amortizedCursor, limit)
	return evicted
}

// cleanupExpiredFrom 是两种清理共用的扫描逻辑，cursor 由调用方指定。
func (c *Cache) cleanupExpiredFrom(cursor **list.Element, limit int) (evicted []EvictItem, finished bool) {
	if limit <= 0 {
		limit = 1
	}
	now := time.Now()
	e := *cursor
	if e == nil {
		e = c.ll.Back()
	}
	scanned := 0
	for e != nil && scanned < limit {
		prev := e.Prev()
		kv := e.Value.(*entry)
		scanned++
		if !kv.expiresAt.IsZero() && now.After(kv.expiresAt) {
			c.removeElement(e)
			evicted = append(evicted, EvictItem{Key: kv.key, Value: kv.value})
		}
		e = prev
	}
	*cursor = e
	return evicted, e == nil
}
