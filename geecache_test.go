package geecache

import (
	"fmt"
	"log"
	"reflect"
	"testing"
)

func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get("key"); !reflect.DeepEqual(v, expect) {
		t.Errorf("callback failed")
	}

}

func TestMetrics(t *testing.T) {
	// 保存测试前的指标，只关心增量
	snapBefore := globalMetrics.Snapshot()

	// 创建很小的缓存（10 字节），确保写入几个 key 后就触发淘汰
	g := NewGroup("m", 10, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("hello-" + key), nil // 每个 value 约 7-8 字节
		},
	))

	// 第一次 Get("a"): miss → local load → 缓存 a（key 1B + value 7B = 8B）
	g.Get("a")
	// 第二次 Get("a"): hit
	g.Get("a")
	// Get("b"): miss → local load → 写入 b（总 16B > 10B → 淘汰 a）
	g.Get("b")
	// Get("c"): miss → local load → 写入 c（再淘汰一个）
	g.Get("c")

	s := globalMetrics.Snapshot()
	hits := s.Hits - snapBefore.Hits
	misses := s.Misses - snapBefore.Misses
	localLoads := s.LocalLoads - snapBefore.LocalLoads
	evictions := s.Evictions - snapBefore.Evictions

	if hits < 1 {
		t.Fatalf("expected at least 1 hit, got %d", hits)
	}
	if misses < 3 {
		t.Fatalf("expected at least 3 misses, got %d", misses)
	}
	if localLoads < 3 {
		t.Fatalf("expected at least 3 local loads, got %d", localLoads)
	}
	if evictions < 1 {
		t.Fatalf("expected at least 1 eviction, got %d", evictions)
	}
	rate := s.HitRate()
	if rate < 0 || rate > 1 {
		t.Fatalf("hit rate out of range: %.4f", rate)
	}
	t.Logf("hits=%d misses=%d loads=%d evictions=%d hit_rate=%.2f",
		hits, misses, localLoads, evictions, rate)
}

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := NewGroup("scores", 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key] += 1
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		if view, err := gee.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		} // load from callback function
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		} // cache hit
	}

	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view)
	}
}
