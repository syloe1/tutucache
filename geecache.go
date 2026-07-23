package geecache

import (
	"fmt"
	pb "geecache/geecachepb"
	"geecache/singleflight"
	"log"
	"sync"
	"time"
)

// 提供被其他节点访问的能力
type Getter interface {
	Get(key string) ([]byte, error)
}

type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// group是独立缓存命名空间
type Group struct {
	name      string
	getter    Getter // 缓存穿透回调：缓存查不到时，去哪里加载原始数据（DB/本地文件）
	mainCache cache  // 单机并发LRU缓存（之前带sync.Mutex封装的缓存层）
	peers     PeerPicker
	//用来合并并发请求
	loader *singleflight.Group
	// DefaultTTL 默认过期时间，0 表示永不过期
	DefaultTTL time.Duration
}

func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()

	g := &Group{
		name:   name,
		getter: getter,
		mainCache: cache{
			cacheBytes: cacheBytes,
		},
		loader: singleflight.NewGroup(),
	}
	groups[name] = g
	return g
}
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	//查询本地并发LRU缓存
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		globalMetrics.RecordHit()
		return v, nil
	}
	//缓存未命中， 进入加载逻辑
	globalMetrics.RecordMiss()
	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			//用一致性hash挑选远程节点
			if peer, ok := g.peers.PickPeer(key); ok {
				//发起HTTP请求从远程节点获取数据
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		//key是本地 / 远程节点请求失败
		return g.getLocal(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// 向远程GeeCache节点发起网络请求，获取缓存
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	globalMetrics.RecordPeerLoad()
	//包装成ByteView
	return ByteView{b: res.Value}, nil
}
func (g *Group) getLocal(key string) (ByteView, error) {
	globalMetrics.RecordLocalLoad()
	//使用业务回调拉取原始数据
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	//封装为只读ByteView, 深拷贝防止外部篡改
	value := ByteView{
		//深拷贝
		b: cloneBytes(bytes),
	}
	g.populateCache(key, value)
	return value, nil
}

// 写入本地缓存
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value, g.DefaultTTL)
}

// SetTTL 设置默认过期时间并启动后台清理
// cleanupInterval 为 0 时不启动后台清理（仅依赖 Get 时的惰性删除）
func (g *Group) SetTTL(ttl time.Duration, cleanupInterval time.Duration) {
	g.DefaultTTL = ttl
	if cleanupInterval > 0 {
		g.mainCache.StartCleanup(cleanupInterval)
	}
}
