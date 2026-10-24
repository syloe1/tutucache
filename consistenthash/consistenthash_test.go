package consistenthash

import (
	"strconv"
	"strings"
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
	for _, h := range []uint32{4, 14, 24} {
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

	// 同一节点被重复 Add 时，不应产生重复的虚拟节点；Remove 应清掉它的全部虚拟节点。
	dup := New(3, nil)
	dup.Add("x")
	dup.Add("x")
	if len(dup.keys) != 3 {
		t.Fatalf("duplicate Add must be idempotent, got %d virtual nodes: %v", len(dup.keys), dup.keys)
	}
	dup.Remove("x")
	if len(dup.keys) != 0 || dup.Get("key") != "" {
		t.Fatalf("duplicate Add then Remove left %v", dup.keys)
	}
}

func TestAddCollision(t *testing.T) {
	// 所有虚拟节点哈希值都为 0，制造节点间碰撞。
	m := New(1, func([]byte) uint32 { return 0 })
	m.Add("a", "b")

	// 先加入者占槽：环上只有一条记录，不会被 b 覆盖，也不留重复 key。
	if len(m.keys) != 1 || m.keys[0] != 0 {
		t.Fatalf("collision should leave exactly one ring slot, got %v", m.keys)
	}
	if owner := m.hashMap[0]; owner != "a" {
		t.Fatalf("first node should keep the colliding slot, got owner %q", owner)
	}
	if _, ok := m.nodeHashes["b"]; ok {
		t.Fatalf("node b should not record a slot it never got: %v", m.nodeHashes["b"])
	}
	if got := m.Get("key"); got != "a" {
		t.Fatalf("colliding slot should belong to a, got %q", got)
	}

	// 删除占槽节点后环为空；被跳过的 b 不会事后继承该槽。
	m.Remove("a")
	if len(m.keys) != 0 || m.Get("key") != "" {
		t.Fatalf("removing the owner should empty the ring, got keys=%v", m.keys)
	}
}

func TestHashUint32Range(t *testing.T) {
	// 哈希值始终是 uint32：0xFFFFFFFF 在 32 位平台上不得变成负数或改变排序。
	const maxUint32 = ^uint32(0)
	m := New(1, func(key []byte) uint32 {
		if strings.HasSuffix(string(key), "high") {
			return maxUint32
		}
		return 1
	})
	m.Add("low", "high")

	if len(m.keys) != 2 || m.keys[0] != 1 || m.keys[1] != maxUint32 {
		t.Fatalf("ring should keep uint32 ordering, got %v", m.keys)
	}
	if got := m.Get("low"); got != "low" {
		t.Fatalf("expect low to own hash 1, got %q", got)
	}
	if got := m.Get("high"); got != "high" {
		t.Fatalf("expect high to own hash 0xFFFFFFFF, got %q", got)
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
