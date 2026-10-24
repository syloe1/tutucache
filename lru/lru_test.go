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
	lru := New(int64(0))
	lru.Add("key1", String("1234"), 0)
	if v, ok, evicted := lru.Get("key1"); !ok || string(v.(String)) != "1234" || len(evicted) != 0 {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if v, ok, evicted := lru.Get("key2"); ok || v != nil || len(evicted) != 0 {
		t.Fatalf("cache miss key2 failed")
	}
}
func TestRemoveoldest(t *testing.T) {
	k1, k2, k3 := "key1", "key2", "k3"
	v1, v2, v3 := "value1", "value2", "v3"
	cap := len(k1 + k2 + v1 + v2)
	lru := New(int64(cap))
	lru.Add(k1, String(v1), 0)
	lru.Add(k2, String(v2), 0)
	lru.Add(k3, String(v3), 0)

	if _, ok, _ := lru.Get("key1"); ok || lru.Len() != 2 {
		t.Fatalf("Removeoldest key1 failed")
	}
}

func TestAddReturnsEvicted(t *testing.T) {
	// 保存被淘汰的key
	keys := make([]string, 0)
	// 收集 Add 返回的淘汰项
	collect := func(items []EvictItem) {
		for _, item := range items {
			keys = append(keys, item.Key)
		}
	}
	// 最大总内存限制10字节
	lru := New(int64(10))

	collect(lru.Add("key1", String("123456"), 0)) // key1:4 + val6 = 10字节
	collect(lru.Add("k2", String("k2"), 0))       // 新增后总内存超10，淘汰key1
	collect(lru.Add("k3", String("k3"), 0))       // 内存充足，不淘汰
	collect(lru.Add("k4", String("k4"), 0))       // 再次超限，淘汰k2

	expect := []string{"key1", "k2"}

	// 切片深度对比，验证淘汰顺序是否正确
	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("evicted keys=%v, want %v", keys, expect)
	}
}

func TestBytes(t *testing.T) {
	c := New(int64(0))
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
	lru := New(int64(100))

	// 写入一个 10ms 后过期的 key
	lru.Add("short", String("live"), 10*time.Millisecond)
	// 写入永不过期的 key
	lru.Add("forever", String("always"), 0)

	// 立即 Get，应该都能命中
	if _, ok, _ := lru.Get("short"); !ok {
		t.Fatal("short should exist before TTL")
	}
	if _, ok, _ := lru.Get("forever"); !ok {
		t.Fatal("forever should exist")
	}

	// 等 20ms，short 应该过期
	time.Sleep(20 * time.Millisecond)

	v, ok, evicted := lru.Get("short")
	if ok || v != nil {
		t.Fatal("short should be expired after TTL")
	}
	if len(evicted) != 1 || evicted[0].Key != "short" {
		t.Fatalf("expired item not returned: %v", evicted)
	}
	if _, ok, _ := lru.Get("forever"); !ok {
		t.Fatal("forever should still exist")
	}
	// 惰性过期删除后，占用应立即回落（只剩 forever: 7 + 6 = 13）
	if got := lru.Bytes(); got != 13 {
		t.Fatalf("bytes after lazy expiry=%d, want 13", got)
	}
}

func TestCleanupExpired(t *testing.T) {
	lru := New(int64(1000))
	lru.Add("k1", String("v1"), 10*time.Millisecond)
	lru.Add("k2", String("v2"), 10*time.Millisecond)
	lru.Add("k3", String("v3"), 0)

	time.Sleep(20 * time.Millisecond)
	evicted := lru.CleanupExpired()
	if len(evicted) != 2 {
		t.Fatalf("expect delete 2 expired keys, got %d", len(evicted))
	}
	if lru.Len() != 1 {
		t.Fatalf("expect len=1")
	}
	// 主动清理同样应更新占用（只剩 k3: 2 + 2 = 4）
	if got := lru.Bytes(); got != 4 {
		t.Fatalf("bytes after cleanup=%d, want 4", got)
	}
	keys := make([]string, 0, len(evicted))
	for _, item := range evicted {
		keys = append(keys, item.Key)
	}
	if !reflect.DeepEqual([]string{"k1", "k2"}, keys) {
		t.Fatalf("expired keys=%v, want [k1 k2]", keys)
	}
}

func TestCleanupExpiredBatchPages(t *testing.T) {
	c := New(int64(1000))
	for i := 1; i <= 5; i++ {
		c.Add(fmt.Sprintf("k%d", i), String("v"), 10*time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	var keys []string
	// 每批最多看 2 个节点：5 个条目应分成 2 + 2 + 1 三批。
	for batch := 0; batch < 3; batch++ {
		evicted, finished := c.CleanupExpiredBatch(2)
		if len(evicted) > 2 {
			t.Fatalf("batch %d evicted %d items, want <= 2", batch, len(evicted))
		}
		for _, item := range evicted {
			keys = append(keys, item.Key)
		}
		if wantFinished := batch == 2; finished != wantFinished {
			t.Fatalf("batch %d finished=%v, want %v", batch, finished, wantFinished)
		}
	}
	if c.Len() != 0 {
		t.Fatalf("len=%d, want 0", c.Len())
	}
	// 从尾部（最冷端）向头部扫描，顺序应为 k1..k5。
	if !reflect.DeepEqual([]string{"k1", "k2", "k3", "k4", "k5"}, keys) {
		t.Fatalf("expired keys=%v, want [k1 k2 k3 k4 k5]", keys)
	}
}

func TestCleanupCursorUpdatedWhenEntryExpiresOutOfBand(t *testing.T) {
	c := New(int64(1000))
	c.Add("k1", String("v"), 10*time.Millisecond)
	c.Add("k2", String("v"), 10*time.Millisecond)
	c.Add("k3", String("v"), 10*time.Millisecond)
	c.Add("k4", String("v"), 0) // 永不过期
	time.Sleep(20 * time.Millisecond)

	// 第一页只处理 k1，游标移到 k2。
	evicted, finished := c.CleanupExpiredBatch(1)
	if len(evicted) != 1 || evicted[0].Key != "k1" || finished {
		t.Fatalf("first batch = %v, finished=%v; want [k1], false", evicted, finished)
	}

	// k2 被 Get 的惰性过期路径删除；游标必须跟着退到 k3，
	// 否则下一批会停在已删除节点上，漏掉 k3。
	if _, ok, got := c.Get("k2"); ok || len(got) != 1 || got[0].Key != "k2" {
		t.Fatalf("lazy expiry of k2 = %v, ok=%v", got, ok)
	}

	evicted, finished = c.CleanupExpiredBatch(10)
	if len(evicted) != 1 || evicted[0].Key != "k3" || !finished {
		t.Fatalf("second batch = %v, finished=%v; want [k3], true", evicted, finished)
	}
	if c.Len() != 1 {
		t.Fatalf("len=%d, want 1 (only k4)", c.Len())
	}
}

func TestPurgeResetsCleanupCursor(t *testing.T) {
	c := New(int64(1000))
	c.Add("a", String("v"), time.Hour)
	c.Add("b", String("v"), time.Hour)

	// 让续扫游标停在链表内部，再清空缓存。
	evicted, finished := c.CleanupExpiredBatch(1)
	if len(evicted) != 0 || finished {
		t.Fatalf("first batch = %v, finished=%v; want empty, false", evicted, finished)
	}

	c.Purge()

	c.Add("expired", String("v"), 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	evicted, finished = c.CleanupExpiredBatch(10)
	if !finished || len(evicted) != 1 || evicted[0].Key != "expired" {
		t.Fatalf("after Purge: evicted=%v finished=%v; want [expired], true", evicted, finished)
	}
}

func TestNewRejectsNegativeMaxBytes(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New(-1) should panic, not silently evict every entry")
		}
	}()
	New(-1)
}

func TestZeroMaxBytesIsUnlimited(t *testing.T) {
	c := New(Unlimited)
	for i := 0; i < 100; i++ {
		c.Add(fmt.Sprintf("k%d", i), String("value"), 0)
	}
	if got := c.Len(); got != 100 {
		t.Fatalf("len=%d, want 100 (0 means unlimited)", got)
	}
}

func TestAddUpdateExistKey(t *testing.T) {
	lru := New(int64(1000))
	lru.Add("k1", String("abc"), 0)
	lru.Add("k1", String("abcd"), 0) // 更新value
	if v, ok, _ := lru.Get("k1"); !ok || string(v.(String)) != "abcd" {
		t.Fatal("update value failed")
	}
}

// Purge 清空全部条目，且不属于容量淘汰，不产生 EvictItem。
func TestPurge(t *testing.T) {
	c := New(4)
	c.Add("a", String("a"), 0) // 2 字节
	c.Add("b", String("b"), 0) // 2 字节，总计 4

	c.Purge()

	if c.Len() != 0 || c.Bytes() != 0 {
		t.Fatalf("after Purge: len=%d bytes=%d, want 0/0", c.Len(), c.Bytes())
	}
	if v, ok, evicted := c.Get("a"); ok || v != nil || len(evicted) != 0 {
		t.Fatal("Purge left entry a in the cache")
	}

	// Purge 之后正常容量淘汰仍然工作。
	c.Add("c", String("c"), 0) // 2 字节
	c.Add("d", String("d"), 0) // 总计 4
	evicted := c.Add("e", String("e"), 0)
	if len(evicted) != 1 || evicted[0].Key != "c" {
		t.Fatalf("after Purge: evicted=%v, want [c]", evicted)
	}
}
