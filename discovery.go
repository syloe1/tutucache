package geecache

// ServiceDiscovery 描述节点的注册、注销与变更通知。
type ServiceDiscovery interface {
	// GetPeers 返回当前所有节点地址。
	GetPeers() ([]string, error)
	// Register 注册节点地址。
	Register(add string) error
	// Unregister 注销节点地址。
	Unregister(addr string) error
	// Watch 监听节点列表变更；onChange 由后台 goroutine 调用。
	Watch(onChange func([]string))
}
