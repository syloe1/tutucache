# tutucache 扩展指南

> 本文列出将 tutucache 从教学项目推向**生产级分布式缓存**的完整方向，按优先级和难度分为三个梯队。

---

## 目录

1. [第一梯队：必备增强（P0）](#第一梯队必备增强p0)
   - [1.1 gRPC 通信](#11-grpc-通信)
   - [2.2 TTL 过期机制](#12-ttl-过期机制)
   - [3.3 缓存统计 & 监控](#13-缓存统计--监控)
   - [4.4 优雅关闭 & 健康检查](#14-优雅关闭--健康检查)
2. [第二梯队：生产加固（P1）](#第二梯队生产加固p1)
   - [2.1 服务发现 & 动态节点](#21-服务发现--动态节点)
   - [2.2 Circuit Breaker 熔断](#22-circuit-breaker-熔断)
   - [2.3 TLS / 认证](#23-tls--认证)
   - [2.4 数据持久化](#24-数据持久化)
3. [第三梯队：进阶特性（P2）](#第三梯队进阶特性p2)
   - [3.1 缓存写入策略](#31-缓存写入策略)
   - [3.2 批量操作](#32-批量操作)
   - [3.3 事件通知 & 失效](#33-事件通知--失效)
   - [3.4 数据复制](#34-数据复制)
4. [实施路线图](#实施路线图)

---

## 第一梯队：必备增强（P0）

这些是让 tutucache 成为一个**真正可用的缓存系统**所必需的能力。

### 1.1 gRPC 通信

**现状：** proto 文件中已定义了 `service GroupCache`，但只用了 protobuf 序列化，通信仍是 HTTP。`go.mod` 中已引入 `google.golang.org/grpc`。

**为什么需要：** gRPC 比纯 HTTP 性能更好（HTTP/2 多路复用、二进制帧），且 proto 生成的 gRPC 接口提供了类型安全的调用方式。

**实现方案：**

```go
// peers_grpc.go — 新建文件

// gRPC 服务端
type GRPCServer struct {
    geecachepb.UnimplementedGroupCacheServer
    self string
}

func (s *GRPCServer) Get(ctx context.Context, req *pb.Request) (*pb.Response, error) {
    group := GetGroup(req.Group)
    if group == nil {
        return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
    }
    view, err := group.Get(req.Key)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "%v", err)
    }
    return &pb.Response{Value: view.ByteSlice()}, nil
}

// gRPC 客户端 — 实现 PeerGetter 接口
type grpcGetter struct {
    addr   string
    client geecachepb.GroupCacheClient
}

func (g *grpcGetter) Get(in *pb.Request, out *pb.Response) error {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    resp, err := g.client.Get(ctx, in)
    if err != nil {
        return err
    }
    *out = *resp
    return nil
}

// GRPCPool — 类似 HTTPPool，管理 gRPC 连接池
type GRPCPool struct {
    self     string
    peers    *consistenthash.Map
    getters  map[string]*grpcGetter  // 复用连接
}
```

**扩展点：** 可抽象出 `Transport` 接口，支持 HTTP 和 gRPC 两种实现，通过配置切换：

```go
type Transport interface {
    Serve() error
    PeerGetter
    PeerPicker
}
```

### 1.2 TTL 过期机制

**现状：** 只有 LRU 容量淘汰，没有**时间过期**。一个缓存条目如果很少被访问，可能会在内存中呆很久。

**为什么需要：** 真实场景中数据有"保鲜期"（如用户 session 30 分钟过期、商品价格 5 分钟刷新）。

**实现方案：**

```go
// lru/lru.go — 在 entry 中增加过期时间
type entry struct {
    key       string
    value     Value
    expiresAt time.Time  // ★ 新增：过期时间。零值表示永不过期
}

// 新增带 TTL 的 Add 方法
func (c *Cache) AddWithTTL(key string, value Value, ttl time.Duration) {
    var expiresAt time.Time
    if ttl > 0 {
        expiresAt = time.Now().Add(ttl)
    }
    // ... 原有逻辑，存入 entry{..., expiresAt: expiresAt}
}

// Get 时检查过期
func (c *Cache) Get(key string) (value Value, ok bool) {
    if ele, hit := c.cache[key]; hit {
        kv := ele.Value.(*entry)
        // ★ 惰性过期检查
        if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
            c.removeElement(ele)  // 惰性删除
            return nil, false
        }
        c.ll.MoveToFront(ele)
        return kv.value, true
    }
    return nil, false
}
```

**两种过期策略对比：**

| 策略 | 做法 | 优点 | 缺点 |
|------|------|------|------|
| 惰性删除 | Get 时检查是否过期 | 实现简单，无额外 goroutine | 过期 key 占用内存直到被访问 |
| 主动清理 | 后台 goroutine 定时扫描 | 及时释放内存 | 增加 CPU 开销和复杂度 |

**建议：** 先实现惰性删除，再添加一个可选的主动清理协程：

```go
// 后台清理 goroutine，每 30 秒扫描一次
func (c *Cache) startCleanup(interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        for range ticker.C {
            c.mu.Lock()
            for e := c.ll.Back(); e != nil; e = e.Prev() {
                kv := e.Value.(*entry)
                if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
                    c.removeElement(e)
                }
            }
            c.mu.Unlock()
        }
    }()
}
```

### 1.3 缓存统计 & 监控

**现状：** 没有暴露任何指标，无法知道缓存的命中率、内存使用等。

**为什么需要：** 没有监控的系统是"盲飞"。命中率直接影响要不要扩容、配置是否合理。

**实现方案：**

```go
// metrics.go — 新建文件
type Metrics struct {
    mu        sync.RWMutex
    Hits      int64  // 命中次数
    Misses    int64  // 未命中次数
    LocalLoads int64 // 从本地数据源加载次数
    PeerLoads  int64 // 从远程节点加载次数
    Evictions  int64 // 淘汰条目数
    TotalBytes int64 // 当前占用内存
}

var globalMetrics = &Metrics{}

// 在 Group.Get、load、populateCache 等关键路径埋点
func (m *Metrics) RecordHit()    { atomic.AddInt64(&m.Hits, 1) }
func (m *Metrics) RecordMiss()   { atomic.AddInt64(&m.Misses, 1) }
func (m *Metrics) HitRate() float64 {
    total := atomic.LoadInt64(&m.Hits) + atomic.LoadInt64(&m.Misses)
    if total == 0 { return 0 }
    return float64(atomic.LoadInt64(&m.Hits)) / float64(total)
}

// 通过 HTTP 端点暴露 Prometheus 风格的指标
func (p *HTTPPool) metricsHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "geecache_hits_total %d\n", atomic.LoadInt64(&globalMetrics.Hits))
    fmt.Fprintf(w, "geecache_misses_total %d\n", atomic.LoadInt64(&globalMetrics.Misses))
    fmt.Fprintf(w, "geecache_hit_rate %.4f\n", globalMetrics.HitRate())
    // ...
}
```

**更完整的方案：** 集成 [prometheus/client_golang](https://github.com/prometheus/client_golang)，自动获得 Grafana 面板和告警能力。

### 1.4 优雅关闭 & 健康检查

**现状：** `http.ListenAndServe` 是阻塞调用，没有处理 SIGTERM/SIGINT 信号。进程被 kill 时可能正在处理请求。

**实现方案：**

```go
// cmd/main.go
func main() {
    // ... 原有初始化 ...

    server := &http.Server{Addr: addr[7:], Handler: peers}

    // 在 goroutine 中启动 server
    go func() {
        log.Printf("geecache running at %s", addr)
        if err := server.ListenAndServe(); err != http.ErrServerClosed {
            log.Fatal(err)
        }
    }()

    // 等待退出信号
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    log.Println("Shutting down...")
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    server.Shutdown(ctx)  // 优雅关闭：不接收新请求，等待现有请求完成
}
```

**健康检查端点：**

```go
// http.go — 在 HTTPPool 中增加
func (p *HTTPPool) healthHandler(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // 新增健康检查路由
    if r.URL.Path == p.basePath + "health" {
        p.healthHandler(w, r)
        return
    }
    // ... 原有逻辑 ...
}
```

---

## 第二梯队：生产加固（P1）

### 2.1 服务发现 & 动态节点

**现状：** 节点地址硬编码在 `main.go` 中。新增/下线节点需要重启所有服务。

**实现方案：**

```go
// discovery.go — 新建文件
type ServiceDiscovery interface {
    // 获取所有节点地址
    GetPeers() ([]string, error)
    // 注册当前节点
    Register(addr string) error
    // 监听节点变更
    Watch(onChange func([]string))
}

// 基于 etcd/consul 的实现
type EtcdDiscovery struct {
    client *clientv3.Client
    prefix string // /geecache/nodes/
}
```

**节点变更时的处理：**

```go
// 监听节点变化，动态更新哈希环
func (p *HTTPPool) watchPeers(discovery ServiceDiscovery) {
    discovery.Watch(func(peers []string) {
        p.mu.Lock()
        p.peers = consistenthash.New(defaultReplicas, nil)
        p.peers.Add(peers...)
        // 同步更新 httpGetters
        for _, peer := range peers {
            if _, ok := p.httpGetters[peer]; !ok {
                p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
            }
        }
        p.mu.Unlock()
        log.Printf("[GeeCache] peers updated: %v", peers)
    })
}
```

**可选方案：**

| 方案 | 适用场景 |
|------|----------|
| etcd / consul | 生产环境，需要强一致性 |
| DNS SRV 记录 | Kubernetes 环境（用 Headless Service） |
| 配置文件 + 热加载 | 小规模部署 |
| Redis Pub/Sub | 已有 Redis 设施的团队 |

### 2.2 Circuit Breaker 熔断

**现状：** 远程节点挂了，每次请求都会尝试连接，浪费资源。虽然有降级到本地的逻辑，但没有对故障节点"暂停尝试"的机制。

**实现方案：**

```go
// circuitbreaker.go — 新建文件
// 使用 github.com/sony/gobreaker 或自实现简单版本
type CircuitBreaker struct {
    mu            sync.Mutex
    state         State  // Closed, Open, HalfOpen
    failureCount  int
    failureThresh int           // 连续失败多少次 → 熔断
    timeout       time.Duration // 熔断多久后尝试恢复
    lastFailure   time.Time
}

type State int
const (
    StateClosed   State = iota  // 正常
    StateOpen                    // 熔断中，直接拒绝
    StateHalfOpen                // 半开，探测性放行一个请求
)

// 在 getFromPeer 中接入熔断
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
    // 检查熔断状态
    if breaker.IsOpen() {
        return ByteView{}, ErrCircuitOpen  // 快速失败，降级到本地
    }
    // ... 原有请求逻辑 ...
    if err != nil {
        breaker.RecordFailure()
    } else {
        breaker.RecordSuccess()
    }
}
```

**三种状态转换：**

```
 Closed ──连续N次失败──► Open ──timeout到期──► HalfOpen
   ▲                                              │
   └──────── 请求成功 ◄────────────────────────────┘
                    请求失败 → 重新回到 Open
```

### 2.3 TLS / 认证

**现状：** 节点间 HTTP 通信是明文的，也没有认证机制。在非信任网络中不安全。

**实现方案：**

```go
// http.go — 修改 httpGetter
type httpGetter struct {
    baseURL    string
    httpClient *http.Client
}

func NewHTTPGetter(baseURL string) *httpGetter {
    return &httpGetter{
        baseURL: baseURL,
        httpClient: &http.Client{
            Transport: &http.Transport{
                TLSClientConfig: &tls.Config{
                    // 生产环境加载证书
                    Certificates: []tls.Certificate{loadCert()},
                    RootCAs:      loadCA(),
                },
            },
            Timeout: 2 * time.Second,
        },
    }
}

// 节点间认证：在 HTTP Header 中携带 token
func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
    req, _ := http.NewRequest("GET", u, nil)
    req.Header.Set("X-GeeCache-Token", sharedSecret)
    res, err := h.httpClient.Do(req)
    // ...
}

// ServeHTTP 中校验 token
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.Header.Get("X-GeeCache-Token") != sharedSecret {
        http.Error(w, "unauthorized", http.StatusUnauthorized)
        return
    }
    // ... 原有逻辑 ...
}
```

### 2.4 数据持久化

**现状：** 纯内存缓存，进程重启后所有数据丢失。在节点重启时会引发大量数据库查询（缓存雪崩的一个变体）。

**实现方案：**

```go
// persistence.go — 新建文件
type Persister interface {
    Save(key string, value []byte, expiresAt time.Time) error
    Load(key string) ([]byte, time.Time, error)
    Delete(key string) error
    LoadAll() (map[string]CacheEntry, error)
}

// BoltDB 实现（嵌入式、Go 原生）
type BoltPersister struct {
    db *bolt.DB
}

// Badger 实现（更高性能的嵌入式 KV 存储）
type BadgerPersister struct {
    db *badger.DB
}
```

**启动时的恢复流程：**

```go
func NewGroup(name string, cacheBytes int64, getter Getter, persister Persister) *Group {
    g := &Group{ /* ... */ }

    // 如果配置了持久化，启动时恢复数据
    if persister != nil {
        go g.recoverFromDisk(persister)
    }
    return g
}

func (g *Group) recoverFromDisk(p Persister) {
    entries, err := p.LoadAll()
    if err != nil {
        log.Printf("[GeeCache] recover failed: %v", err)
        return
    }
    for key, entry := range entries {
        g.mainCache.add(key, ByteView{b: cloneBytes(entry.Value)})
    }
    log.Printf("[GeeCache] recovered %d entries from disk", len(entries))
}
```

**注意事项：**
- 持久化会增加写延迟，可考虑**异步批量写入**
- 仅持久化热点数据，使用**采样策略**（如每 10 次写入才持久化一次）
- 设置持久化文件大小上限，使用 LRU 策略淘汰磁盘上的旧数据

---

## 第三梯队：进阶特性（P2）

### 3.1 缓存写入策略

**现状：** 只有 `Get`（读时加载）。没有 Set/Delete/Update 等方法。

**扩展接口：**

```go
// geecache.go — 在 Group 上增加
func (g *Group) Set(key string, value []byte, ttl time.Duration) error {
    bv := ByteView{b: cloneBytes(value)}
    g.mainCache.add(key, bv)
    // 可选：通知其他节点失效
    return nil
}

func (g *Group) Delete(key string) error {
    g.mainCache.remove(key)
    // 可选：广播失效通知
    return nil
}

// 写入模式枚举
type WriteStrategy int
const (
    WriteBack  WriteStrategy = iota  // 先写缓存，异步刷到 DB
    WriteThrough                     // 同步写缓存 + DB
    WriteAround                      // 只写 DB，读时才进缓存
)
```

### 3.2 批量操作

**现状：** 每次只能 Get 一个 key。批量查询需要 N 次调用。

**扩展接口：**

```go
func (g *Group) MGet(keys []string) (map[string]ByteView, error) {
    result := make(map[string]ByteView, len(keys))
    var wg sync.WaitGroup
    var mu sync.Mutex

    for _, key := range keys {
        wg.Add(1)
        go func(k string) {
            defer wg.Done()
            if v, err := g.Get(k); err == nil {
                mu.Lock()
                result[k] = v
                mu.Unlock()
            }
        }(key)
    }
    wg.Wait()
    return result, nil
}
```

**优化方向：**
- 按 key 归属分组，每个远程节点只发一次批量请求
- 在 protobuf 中增加批量接口：

```protobuf
message BatchRequest {
    string group = 1;
    repeated string keys = 2;
}
message BatchResponse {
    map<string, bytes> values = 1;
}
```

### 3.3 事件通知 & 失效

**现状：** 缓存和 DB 之间没有联动。DB 中数据更新后，缓存中的旧数据仍然存在。

**实现方案：**

```go
// 发布订阅模式
type CacheEvent struct {
    Type    EventType  // Set, Delete, Expire
    Group   string
    Key     string
    Version int64     // 乐观锁版本号
}

type EventBus interface {
    Publish(event CacheEvent) error
    Subscribe(group string, handler func(CacheEvent)) error
}

// 监听 DB 变更，主动失效缓存
func (g *Group) ListenDBChanges(bus EventBus) {
    bus.Subscribe(g.name, func(event CacheEvent) {
        switch event.Type {
        case EventDelete, EventUpdate:
            g.mainCache.remove(event.Key)
        }
    })
}
```

**可选方案：**
- Redis Pub/Sub
- Kafka / NATS
- 基于 gRPC stream 的节点间广播

### 3.4 数据复制

**现状：** 每个 key 只存在一个节点上。节点宕机后，对应的缓存全部丢失，需要回源加载。

**实现方案：**

```go
// 在一致性哈希中增加副本
func (m *Map) GetN(key string, n int) []string {
    // 返回顺时针方向的 N 个不同节点
    // 第一个是主节点，其余是备份节点
}

// 写入时复制到 N 个副本
func (g *Group) setWithReplication(key string, value ByteView) {
    nodes := g.peers.GetN(key, 3)  // 3 副本
    for _, node := range nodes {
        if node == g.name {
            g.mainCache.add(key, value)
        } else {
            go g.replicateTo(node, key, value)  // 异步复制
        }
    }
}
```

---

## 实施路线图

```
阶段1 (1-2 周)        阶段2 (2-3 周)         阶段3 (1-2 月)
─────────────────    ──────────────────    ──────────────────
TTL 过期机制          gRPC 通信            批量操作 (MGet/MSet)
缓存统计 & 监控        服务发现 (etcd)      数据复制 (N=3)
优雅关闭              熔断器                 事件通知 (Pub/Sub)
健康检查端点           TLS + 认证            持久化 (异步写入)
日志框架升级           动态节点管理           缓存预热
端到端测试                                        Admin UI
```

---

## 总结

tutucache 目前的架构是一个优秀的**教学模板**，清晰地展示了分布式缓存的核心原理。以上扩展方向覆盖了从"能用"到"好用"再到"生产级"的完整路径。

**选型建议：** 如果你的场景是：
- **个人项目/学习** → 实现 P0 中的 TTL + 统计 + 健康检查
- **团队内部工具** → 加上 P1 中的服务发现 + 熔断
- **公司核心服务** → 全部 P0+P1，按需选择 P2 中的特性

记住一个原则：**不要过度设计**。先把核心功能做扎实，根据实际使用中的瓶颈来决定下一步做什么。
