package geecache

type ByteView struct {
	b []byte
}

func (v ByteView) Len() int {
	return len(v.b)
}

// 返回字节拷贝
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}
func (v ByteView) String() string {
	return string(v.b)
}

// 。深拷贝
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
