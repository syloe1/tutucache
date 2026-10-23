package lru

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type String string

func (d String) Len() int {

	return len(d)
}

func TestGet(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add("key1", String("1234"), 0)
	if v, ok := lru.Get("key1"); !ok || string(v.(String)) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := lru.Get("key2"); ok {
		t.Fatalf("cache miss key2 failed")
	}
}
func TestRemoveoldest(t *testing.T) {
	k1, k2, k3 := "key1", "key2", "k3"
	v1, v2, v3 := "value1", "value2", "v3"
	cap := len(k1 + k2 + v1 + v2)
	lru := New(int64(cap), nil)
	lru.Add(k1, String(v1), 0)
	lru.Add(k2, String(v2), 0)
	lru.Add(k3, String(v3), 0)

	if _, ok := lru.Get("key1"); ok || lru.Len() != 2 {
		t.Fatalf("Removeoldest key1 failed")
	}
}

func TestOnEvicted(t *testing.T) {
	// 保存被淘汰的key
	keys := make([]string, 0)
	// 定义淘汰回调
	callback := func(key string, value Value) {
		keys = append(keys, key)
	}
	// 最大总内存限制10字节
	lru := New(int64(10), callback)

	lru.Add("key1", String("123456"), 0) // key1:4 + val6 = 10字节
	lru.Add("k2", String("k2"), 0)       // 新增后总内存超10，淘汰key1
	lru.Add("k3", String("k3"), 0)       // 内存充足，不淘汰
	lru.Add("k4", String("k4"), 0)       // 再次超限，淘汰k2

	expect := []string{"key1", "k2"}

	// 切片深度对比，验证淘汰顺序是否正确
	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("Call OnEvicted failed, expect keys equals to %v", expect)
	}
}

func TestBytes(t *testing.T) {
	c := New(int64(0), nil)
	if got := c.Bytes(); got != 0 {
		t.Fatalf("empty cache bytes=%d, want 0", got)
	}

	c.Add("key1", String("1234"), 0) // 4 + 4 = 8
	c.Add("k2", String("v2"), 0)     // 2 + 2 = 4 → 12
	if got := c.Bytes(); got != 12 {
		t.Fatalf("bytes=%d, want 12", got)
	}

	// 更新已有 key 只按差值调整占用：key1 从 8 变为 5，总量 9
	c.Add("key1", String("1"), 0)
	if got := c.Bytes(); got != 9 {
		t.Fatalf("bytes after update=%d, want 9", got)
	}

	// key1 更新后成为最近访问，淘汰的是 k2，只剩 key1 的 5 字节
	c.RemoveOldest()
	if got := c.Bytes(); got != 5 {
		t.Fatalf("bytes after eviction=%d, want 5", got)
	}
}

func TestTTLExpiry(t *testing.T) {
	lru := New(int64(100), nil)

	// 写入一个 10ms 后过期的 key
	lru.Add("short", String("live"), 10*time.Millisecond)
	// 写入永不过期的 key
	lru.Add("forever", String("always"), 0)

	// 立即 Get，应该都能命中
	if _, ok := lru.Get("short"); !ok {
		t.Fatal("short should exist before TTL")
	}
	if _, ok := lru.Get("forever"); !ok {
		t.Fatal("forever should exist")
	}

	// 等 20ms，short 应该过期
	time.Sleep(20 * time.Millisecond)

	if _, ok := lru.Get("short"); ok {
		t.Fatal("short should be expired after TTL")
	}
	if _, ok := lru.Get("forever"); !ok {
		t.Fatal("forever should still exist")
	}
	// 惰性过期删除后，占用应立即回落（只剩 forever: 7 + 6 = 13）
	if got := lru.Bytes(); got != 13 {
		t.Fatalf("bytes after lazy expiry=%d, want 13", got)
	}
}

func TestCleanupExpired(t *testing.T) {
	keys := make([]string, 0)
	callback := func(key string, v Value) {
		keys = append(keys, key)
	}
	lru := New(int64(1000), callback)
	lru.Add("k1", String("v1"), 10*time.Millisecond)
	lru.Add("k2", String("v2"), 10*time.Millisecond)
	lru.Add("k3", String("v3"), 0)

	time.Sleep(20 * time.Millisecond)
	delCnt := lru.CleanupExpired()
	if delCnt != 2 {
		t.Fatalf("expect delete 2 expired keys, got %d", delCnt)
	}
	if lru.Len() != 1 {
		t.Fatalf("expect len=1")
	}
	// 主动清理同样应更新占用（只剩 k3: 2 + 2 = 4）
	if got := lru.Bytes(); got != 4 {
		t.Fatalf("bytes after cleanup=%d, want 4", got)
	}
	if !reflect.DeepEqual([]string{"k1", "k2"}, keys) {
		t.Fatalf("expire callback keys wrong")
	}
}

func TestNewRejectsNegativeMaxBytes(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New(-1) should panic, not silently evict every entry")
		}
	}()
	New(-1, nil)
}

func TestZeroMaxBytesIsUnlimited(t *testing.T) {
	c := New(Unlimited, nil)
	for i := 0; i < 100; i++ {
		c.Add(fmt.Sprintf("k%d", i), String("value"), 0)
	}
	if got := c.Len(); got != 100 {
		t.Fatalf("len=%d, want 100 (0 means unlimited)", got)
	}
}

func TestAddUpdateExistKey(t *testing.T) {
	lru := New(int64(1000), nil)
	lru.Add("k1", String("abc"), 0)
	lru.Add("k1", String("abcd"), 0) // 更新value
	if v, ok := lru.Get("k1"); !ok || string(v.(String)) != "abcd" {
		t.Fatal("update value failed")
	}
}

// 清空全部条目、不触发淘汰回调 OnEvicted
/*
- `OnEvicted`：LRU 的淘汰回调，只有**容量满了、淘汰旧 key**的时候才会触发；
- Purge 是手动清空，不属于容量淘汰，**规定不能触发这个回调**（这是这个测试里非常关键的业务约定）。
*/
func TestPurge(t *testing.T) {
	var evicted int
	c := New(4, func(string, Value) { evicted++ })
	c.Add("a", String("a"), 0)
	c.Add("b", String("b"), 0)

	c.Purge()

	if c.Len() != 0 || c.Bytes() != 0 {
		t.Fatalf("after Purge: len=%d bytes=%d, want 0/0", c.Len(), c.Bytes())
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("Purge left entry a in the cache")
	}
	if evicted != 0 {
		t.Fatalf("Purge invoked OnEvicted %d times, want 0", evicted)
	}

	// Purge 不能丢掉 OnEvicted：之后的正常容量淘汰仍应触发回调。
	c.Add("c", String("c"), 0)
	c.Add("d", String("d"), 0)
	c.Add("e", String("e"), 0)
	if evicted == 0 {
		t.Fatal("OnEvicted was lost after Purge: capacity eviction did not invoke it")
	}
}
