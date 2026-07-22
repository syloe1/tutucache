package geecache

import (
	pb "geecache/geecachepb"
)

// PeerPicker：用一致性哈希，找到key属于哪台服务器
// 负责【挑选节点】
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter：负责【和选中的远程节点通信，获取缓存】
type PeerGetter interface {
	Get(in *pb.Request, out *pb.Response) error
}
