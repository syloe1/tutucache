package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash 自定义哈希函数类型
type Hash func(data []byte) uint32

// Map 一致性哈希环
type Map struct {
	hash     Hash           // 哈希函数
	replicas int            // 每个真实节点对应的虚拟节点数量
	keys     []int          // 哈希环上所有虚拟节点哈希值，升序排列
	hashMap  map[int]string // 虚拟节点hash -> 真实节点名
}

// New 创建哈希环，replicas 虚拟节点数，fn 哈希函数
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	// 未传入哈希函数，默认 crc32
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 添加真实节点，支持多个
func (m *Map) Add(keys ...string) {
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
		}
	}
	//让这些虚拟节点哈希值， 升序排列
	// 哈希值排序，构建有序环
	sort.Ints(m.keys)
}

// 一个真实节点 -> 多个虚拟节点， 虚拟节点和虚拟节点hash 1->1
// Get 根据key找到对应的真实节点
func (m *Map) Get(key string) string {
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

//cc
