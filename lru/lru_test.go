package lru

import (
	"reflect"
	"testing"
)

type String string

func (d String) Len() int {

	return len(d)
}

func TestGet(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add("key1", String("1234"))
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
	lru.Add(k1, String(v1))
	lru.Add(k2, String(v2))
	lru.Add(k3, String(v3))

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

	lru.Add("key1", String("123456")) // key1:4 + val6 = 10字节
	lru.Add("k2", String("k2"))       // 新增后总内存超10，淘汰key1
	lru.Add("k3", String("k3"))       // 内存充足，不淘汰
	lru.Add("k4", String("k4"))       // 再次超限，淘汰k2

	expect := []string{"key1", "k2"}

	// 切片深度对比，验证淘汰顺序是否正确
	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("Call OnEvicted failed, expect keys equals to %v", expect)
	}
}
