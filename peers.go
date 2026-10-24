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

// PeerBreakerProvider 是 PeerGetter 的可选接口，为每个 (Group, peer) 提供独立熔断器。
// 内置的 httpGetter/grpcGetter 都实现了它，因此某个节点连续失败只会熔断该节点
// 在当前 Group 上的请求，不会再连带熔断路由到其他健康节点的请求。
// 返回 nil 表示退回 Group 级兜底熔断器（兼容未实现该接口的自定义 PeerGetter）。
type PeerBreakerProvider interface {
	BreakerFor(group string) *CircuitBreaker
}

//cc
