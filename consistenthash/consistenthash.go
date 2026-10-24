package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Hash 自定义哈希函数类型
type Hash func(data []byte) uint32

// Map 一致性哈希环。内部自带读写锁，可安全并发使用；
// 由于包含 sync.RWMutex，Map 不可按值拷贝。
type Map struct {
	mu         sync.RWMutex
	hash       Hash             // 哈希函数
	replicas   int              // 每个真实节点对应的虚拟节点数量
	keys       []int            // 哈希环上所有虚拟节点哈希值，升序排列
	hashMap    map[int]string   // 虚拟节点hash -> 真实节点名
	nodeHashes map[string][]int // 真实节点 -> 它占用的虚拟节点哈希值，用于精确删除
}

// New 创建哈希环，replicas 虚拟节点数，fn 哈希函数
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas:   replicas,
		hash:       fn,
		hashMap:    make(map[int]string),
		nodeHashes: make(map[string][]int),
	}
	// 未传入哈希函数，默认 crc32
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 添加真实节点，支持多个
func (m *Map) Add(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nodeHashes == nil {
		m.nodeHashes = make(map[string][]int)
	}
	//key是服务地址
	for _, key := range keys {
		// 每个真实节点生成 replicas 个虚拟节点
		for i := 0; i < m.replicas; i++ {
			// 虚拟节点标识：数字i拼接节点名，区分同一个节点的不同副本
			virtualKey := strconv.Itoa(i) + key
			hashVal := int(m.hash([]byte(virtualKey)))
			//keys 存储的是所有虚拟节点的哈希值
			m.keys = append(m.keys, hashVal)
			m.hashMap[hashVal] = key
			// 记录归属，Remove 时据此只删除本节点的虚拟节点
			m.nodeHashes[key] = append(m.nodeHashes[key], hashVal)
		}
	}
	//让这些虚拟节点哈希值， 升序排列
	// 哈希值排序，构建有序环
	sort.Ints(m.keys)
}

// Remove 删除节点及其全部虚拟节点。节点不存在时为空操作。
func (m *Map) Remove(node string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hashes, ok := m.nodeHashes[node]
	if !ok || len(hashes) == 0 {
		return
	}
	delete(m.nodeHashes, node)

	// 同一节点的虚拟节点可能（因哈希碰撞）落到同一个哈希值上，按出现次数删除。
	counts := make(map[int]int, len(hashes))
	removeSet := make(map[int]struct{}, len(hashes))
	for _, h := range hashes {
		counts[h]++
		removeSet[h] = struct{}{}
	}

	// 原地过滤：m.keys 本身有序，过滤后仍有序，无需再排序。
	// 只删除属于本节点的 count 个条目，其它节点共享同一哈希值的条目保留。
	leftover := make(map[int]int, len(removeSet))
	kept := m.keys[:0]
	for _, h := range m.keys {
		if counts[h] > 0 {
			counts[h]--
			continue
		}
		kept = append(kept, h)
		if _, ok := removeSet[h]; ok {
			leftover[h]++
		}
	}
	m.keys = kept

	// 哈希碰撞时 hashMap 只记录最后一个写入者：只有本节点是当前 owner 时才改，
	// 否则直接跳过，避免误删其它节点（#25 的碰撞语义）。
	for h := range removeSet {
		if m.hashMap[h] != node {
			continue
		}
		if leftover[h] > 0 {
			if owner := m.findOwner(h, node); owner != "" {
				m.hashMap[h] = owner
				continue
			}
		}
		delete(m.hashMap, h)
	}
}

// findOwner 在剩余节点中查找占用哈希值 hashVal 的节点。调用方必须持有 m.mu。
func (m *Map) findOwner(hashVal int, exclude string) string {
	for node, hashes := range m.nodeHashes {
		if node == exclude {
			continue
		}
		for _, h := range hashes {
			if h == hashVal {
				return node
			}
		}
	}
	return ""
}

// 一个真实节点 -> 多个虚拟节点， 虚拟节点和虚拟节点hash 1->1
// Get 根据key找到对应的真实节点
func (m *Map) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.keys) == 0 {
		return ""
	}
	// 计算key的哈希
	hashVal := int(m.hash([]byte(key)))
	// 二分查找第一个 >= hashVal 的虚拟节点下标
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hashVal
	})
	// 环形取模，超出尾部则取第一个节点
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
