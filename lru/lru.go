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

// Cache 带TTL过期能力的LRU缓存
// 内存上限maxBytes；同时支持惰性过期(Get时检查) + 主动全量扫描过期(CleanupExpired)
type Cache struct {
	maxBytes  int64                         // 最大允许占用字节数，Unlimited(0)代表不限制内存
	nbytes    int64                         // 当前已经占用总内存(key+value)
	ll        *list.List                    // 双向链表：Front是最近访问，Back是最久未访问
	cache     map[string]*list.Element      // 哈希表，key映射链表节点，O(1)查找
	OnEvicted func(key string, value Value) // 元素被淘汰/过期删除时的回调
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
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	if maxBytes < 0 {
		panic(fmt.Sprintf("lru: maxBytes must be >= 0 (0 means unlimited), got %d", maxBytes))
	}
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// Purge 清空所有缓存条目。调用方必须自行同步并发访问。
// 手动清理不触发容量淘汰，因此不会调用 OnEvicted 回调。
func (c *Cache) Purge() {
	c.ll.Init()
	c.cache = make(map[string]*list.Element)
	c.nbytes = 0
}

// Get 根据key读取缓存
// 惰性过期：命中时检查过期；已过期直接删除，返回ok=false
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*entry)
		// 判断是否过期
		if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
			c.ll.Remove(ele)
			delete(c.cache, kv.key)
			c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
			if c.OnEvicted != nil {
				c.OnEvicted(kv.key, kv.value)
			}
			return nil, false
		}
		// 命中未过期，移到链表头部（最近访问）
		c.ll.MoveToFront(ele)
		return kv.value, true
	}
	return
}

// RemoveOldest 删除最久未访问节点（链表尾部），内存超限时调用
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add 添加/更新缓存key，支持TTL
// ttl>0 设置过期时间；ttl=0代表永不过期
// 添加完成后循环检查内存上限，超出则持续淘汰最旧节点
func (c *Cache) Add(key string, value Value, ttl time.Duration) {
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
		c.RemoveOldest()
	}
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

// CleanupExpired 主动扫描，删除全部已过期条目
// 注意：调用这个方法前，外部必须自行加锁！
// 返回本次清理删除的条目数量
// LRU是 惰性过期， get访问key的时候才校验过期， 对于长期不访问的过期冷key不会自动释放内存
func (c *Cache) CleanupExpired() int {
	count := 0
	// 从尾部向前遍历，避免删除节点导致迭代器失效
	for e := c.ll.Back(); e != nil; {
		kv := e.Value.(*entry)
		prev := e.Prev()
		if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
			c.ll.Remove(e)          //移除双链表
			delete(c.cache, kv.key) //删除map
			c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
			if c.OnEvicted != nil {
				c.OnEvicted(kv.key, kv.value)
			}
			count++
		}
		e = prev
	}
	return count
}
