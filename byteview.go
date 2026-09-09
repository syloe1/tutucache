package geecache

// ByteView 缓存值的只读视图，实现 lru.Value 接口
// b 存储原始字节切片，对外不直接暴露底层数组，防止外部修改缓存内容
type ByteView struct {
	b []byte
}

// Len 实现 lru.Value 接口，返回字节长度，LRU用来统计内存占用
func (v ByteView) Len() int {
	return len(v.b)
}

// ByteSlice 返回一份底层字节的拷贝（深拷贝）
// 不直接return v.b，避免外部拿到切片引用后修改底层数组，污染缓存内数据
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

// String 将字节转为字符串返回，方便打印、调试
func (v ByteView) String() string {
	return string(v.b)
}

// cloneBytes 对[]byte做深拷贝，生成独立新切片
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
