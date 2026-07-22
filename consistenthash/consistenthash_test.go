package consistenthash

import (
	"strconv"
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
