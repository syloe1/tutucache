package lru

import (
	"container/list"
	"time"
)

type Cache struct {
	maxBytes int64
	nbytes   int64 //已经使用的内存
	ll       *list.List
	cache    map[string]*list.Element
	//淘汰回调函数
	OnEvicted func(key string, value Value) //记录被移除时的回调函数
}

// list链表只能存一个任意变量， 但是我们同时需要key value, 所以我们封装一个entry结构体进去
type entry struct {
	key       string
	value     Value
	expiresAt time.Time //过期时间， 零代表永不过期
}

type Value interface {
	Len() int
}

func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		//断言转换为结构体
		kv := ele.Value.(*entry)
		// 惰性过期：Get 时发现已过期，当场删除
		if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
			c.ll.Remove(ele)
			delete(c.cache, kv.key)
			c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
			if c.OnEvicted != nil {
				c.OnEvicted(kv.key, kv.value)
			}
			return nil, false
		}
		//命中，把节点移动到头部
		c.ll.MoveToFront(ele)
		return kv.value, true
	}
	return
}
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		//删除节点
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		//同步删除key
		delete(c.cache, kv.key)
		//总占用内存 - 这条key + value大小
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		//执行淘汰回调
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// 更新缓存
func (c *Cache) Add(key string, value Value, ttl time.Duration) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		//delta = 新value大小 - 旧value大小
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
		kv.expiresAt = expiresAt
	} else {
		ele := c.ll.PushFront(&entry{key: key, value: value, expiresAt: expiresAt})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}
func (c *Cache) Len() int {
	return c.ll.Len()
}

// CleanupExpired 扫描并删除所有已过期的条目（调用方负责加锁）
func (c *Cache) CleanupExpired() int {
	count := 0
	for e := c.ll.Back(); e != nil; {
		kv := e.Value.(*entry)
		prev := e.Prev()
		if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
			c.ll.Remove(e)
			delete(c.cache, kv.key)
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
