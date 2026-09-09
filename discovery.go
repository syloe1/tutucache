package geecache

type ServiceDiscovery interface {
	//获取所有节点的地址
	GetPeers() ([]string, error)
	//注册当前节点
	Register(add string) error
	//监听节点变更
	Watch(onChange func([]string))
}

//cc
