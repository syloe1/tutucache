package consistenthash

import (
	"hash/crc32"
	"slices"
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
	hash       Hash                // 哈希函数
	replicas   int                 // 每个真实节点对应的虚拟节点数量
	keys       []uint32            // 哈希环上所有虚拟节点哈希值，升序排列
	hashMap    map[uint32]string   // 虚拟节点hash -> 真实节点名（每个 hash 只有一个 owner）
	nodeHashes map[string][]uint32 // 真实节点 -> 它占用的虚拟节点哈希值，用于精确删除
}

// New 创建哈希环，replicas 虚拟节点数，fn 哈希函数
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas:   replicas,
		hash:       fn,
		hashMap:    make(map[uint32]string),
		nodeHashes: make(map[string][]uint32),
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
		m.nodeHashes = make(map[string][]uint32)
	}
	//key是服务地址
	for _, key := range keys {
		// 每个真实节点生成 replicas 个虚拟节点
		for i := 0; i < m.replicas; i++ {
			// 虚拟节点标识：数字i拼接节点名，区分同一个节点的不同副本。
			// 保持原有拼接格式，避免升级后整环重新分布。
			virtualKey := strconv.Itoa(i) + key
			hashVal := m.hash([]byte(virtualKey))
			// 哈希碰撞：先加入者占槽，后来者跳过，避免覆盖已有 owner
			// 或留下重复的环条目。该虚拟节点因此不参与环，节点可能少于 replicas 个槽。
			if _, exists := m.hashMap[hashVal]; exists {
				continue
			}
			//keys 存储的是所有虚拟节点的哈希值
			m.keys = append(m.keys, hashVal)
			m.hashMap[hashVal] = key
			// 记录归属，Remove 时据此只删除本节点的虚拟节点
			m.nodeHashes[key] = append(m.nodeHashes[key], hashVal)
		}
	}
	// 哈希值升序排列，构建有序环
	slices.Sort(m.keys)
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

	// Add 保证一个 hash 只有一个 owner，因此 hashes 中的每个值都归本节点独占，
	// 不存在需要按出现次数删除或转交其它节点的共享槽。
	removeSet := make(map[uint32]struct{}, len(hashes))
	for _, h := range hashes {
		removeSet[h] = struct{}{}
	}

	// 原地过滤：m.keys 本身有序，过滤后仍有序，无需再排序。
	kept := m.keys[:0]
	for _, h := range m.keys {
		if _, drop := removeSet[h]; !drop {
			kept = append(kept, h)
		}
	}
	m.keys = kept

	for h := range removeSet {
		delete(m.hashMap, h)
	}
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
	hashVal := m.hash([]byte(key))
	// 二分查找第一个 >= hashVal 的虚拟节点下标
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hashVal
	})
	// 环形取模，超出尾部则取第一个节点
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
