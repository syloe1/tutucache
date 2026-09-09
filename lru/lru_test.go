package lru

import (
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
	if !reflect.DeepEqual([]string{"k1", "k2"}, keys) {
		t.Fatalf("expire callback keys wrong")
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
