package consistenthash

import (
	"strconv"
	"sync"
	"testing"
)

func TestHashing(t *testing.T) {
	// 自定义哈希函数：直接把字节转数字，方便人工推演哈希环
	hashRing := New(3, func(key []byte) uint32 {
		val, _ := strconv.Atoi(string(key))
		return uint32(val)
	})

	// 添加真实节点 6、4、2，每个生成3个虚拟节点
	// 虚拟节点key：06,16,26 | 04,14,24 | 02,12,22
	// 对应哈希值：6,16,26,4,14,24,2,12,22 → 排序后 [2,4,6,12,14,16,22,24,26]
	hashRing.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2", // hash=2，匹配节点2
		"11": "2", // hash=11，第一个大于等于11是12，对应节点2
		"23": "4", // hash=23，第一个大于等于23是24，对应节点4
		"27": "2", // hash=27，超过所有节点，环形回到第一个2
	}

	for k, wantNode := range testCases {
		if got := hashRing.Get(k); got != wantNode {
			t.Errorf("key=%s expect node %s, but got %s", k, wantNode, got)
		}
	}

	// 新增节点8，虚拟节点：08、18、28，哈希8,18,28
	hashRing.Add("8")
	// 哈希环新增8、18、28，排序后 [2,4,6,8,12,14,16,18,22,24,26,28]
	// key=27 hash=27，现在第一个≥27是28，对应节点8
	testCases["27"] = "8"

	for k, wantNode := range testCases {
		if got := hashRing.Get(k); got != wantNode {
			t.Errorf("key=%s expect node %s, but got %s", k, wantNode, got)
		}
	}
}

func TestRemove(t *testing.T) {
	// 自定义哈希，便于人工推演每一个虚拟节点的位置。
	m := New(3, func(key []byte) uint32 {
		val, _ := strconv.Atoi(string(key))
		return uint32(val)
	})
	m.Add("6", "4", "2")
	// 环：[2(2), 4(4), 6(6), 12(2), 14(4), 16(6), 22(2), 24(4), 26(6)]

	// 先确认这些 key 删除前落在预期节点，避免写出空转断言。
	if got := m.Get("23"); got != "4" {
		t.Fatalf(`key=23 expect node 4 before remove, got %q`, got)
	}
	if got := m.Get("11"); got != "2" {
		t.Fatalf(`key=11 expect node 2 before remove, got %q`, got)
	}
	if got := m.Get("15"); got != "6" {
		t.Fatalf(`key=15 expect node 6 before remove, got %q`, got)
	}

	m.Remove("4")

	// 删除 node 4 后环只剩 [2, 6, 12, 16, 22, 26]，hash=23 应改由 node 6 承接。
	if got := m.Get("23"); got != "6" {
		t.Fatalf(`key=23 expect node 6 after removing node 4, got %q`, got)
	}
	// 原本属于其它节点的 key 不应受影响。
	if got := m.Get("11"); got != "2" {
		t.Fatalf(`key=11 expect node 2 after removing node 4, got %q`, got)
	}
	if got := m.Get("15"); got != "6" {
		t.Fatalf(`key=15 expect node 6 after removing node 4, got %q`, got)
	}

	// 内部状态：node 4 的三个虚拟节点全部移除，其它节点保留。
	if len(m.keys) != 6 {
		t.Fatalf("expect 6 virtual nodes left, got %d: %v", len(m.keys), m.keys)
	}
	for _, h := range []int{4, 14, 24} {
		if _, ok := m.hashMap[h]; ok {
			t.Errorf("hashMap still contains removed vnode %d", h)
		}
	}
	if _, ok := m.nodeHashes["4"]; ok {
		t.Error("nodeHashes still contains removed node 4")
	}

	// 删除剩余节点，环应为空。
	m.Remove("2")
	if _, ok := m.hashMap[2]; ok {
		t.Error("hashMap still contains node 2")
	}
	m.Remove("6")
	if got := m.Get("23"); got != "" {
		t.Fatalf("expect empty ring after removing all nodes, got %q", got)
	}
}

func TestRemoveEdgeCases(t *testing.T) {
	// 空环上删除不存在的节点：旧实现会因负容量 panic。
	New(50, nil).Remove("gone")

	m := New(50, nil)
	m.Add("node1")
	m.Remove("never-added")
	if got := m.Get("key"); got != "node1" {
		t.Fatalf("removing unknown node must not change the ring, got %q", got)
	}
	if len(m.keys) != 50 {
		t.Fatalf("expect 50 virtual nodes, got %d", len(m.keys))
	}

	// 删除最后一个节点后重复删除，必须是幂等空操作。
	only := New(3, nil)
	only.Add("only")
	only.Remove("only")
	only.Remove("only")
	if got := only.Get("key"); got != "" {
		t.Fatalf("expect empty ring, got %q", got)
	}

	// 同一节点被重复 Add 时，Remove 应清掉它的全部虚拟节点。
	dup := New(3, nil)
	dup.Add("x")
	dup.Add("x")
	dup.Remove("x")
	if len(dup.keys) != 0 || dup.Get("key") != "" {
		t.Fatalf("duplicate Add then Remove left %v", dup.keys)
	}
}

func TestRemoveCollision(t *testing.T) {
	// 所有虚拟节点哈希值都为 0，制造节点间碰撞。
	newColliding := func() *Map {
		return New(1, func([]byte) uint32 { return 0 })
	}

	// hashMap 记录最后写入者 b；删除非 owner 的 a 不能误伤 b。
	m := newColliding()
	m.Add("a", "b")
	m.Remove("a")
	if got := m.Get("key"); got != "b" {
		t.Fatalf("removing a must not drop colliding node b, got %q", got)
	}

	// 删除当前 owner b 时，共享哈希槽应转交给仍然存在的 a。
	m = newColliding()
	m.Add("a", "b")
	m.Remove("b")
	if got := m.Get("key"); got != "a" {
		t.Fatalf("colliding slot should fall back to a, got %q", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := New(10, nil)
	m.Add("base1", "base2")

	var wg sync.WaitGroup
	for id := 0; id < 4; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			node := "worker" + strconv.Itoa(id)
			for i := 0; i < 500; i++ {
				m.Add(node)
				m.Get("key-" + strconv.Itoa(i))
				m.Remove(node)
			}
		}(id)
	}
	wg.Wait()

	if got := m.Get("key"); got == "" {
		t.Fatal("base nodes should remain after concurrent add/remove")
	}
}
