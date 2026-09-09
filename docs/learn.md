# tutucache 源码学习指南

> 本文带你从零开始，逐步串联这个分布式缓存项目，理解每一层的设计动机与实现细节。

## 目录

1. [从哪里开始？](#1-从哪里开始)
2. [第一层：LRU 缓存淘汰算法 + TTL 过期](#2-第一层lru-缓存淘汰算法--ttl-过期)
3. [第二层：并发安全的缓存封装](#3-第二层并发安全的缓存封装)
4. [第三层：只读字节视图 ByteView](#4-第三层只读字节视图-byteview)
5. [第四层：核心 Group —— 缓存命名空间](#5-第四层核心-group--缓存命名空间)
6. [第五层：节点间通信（HTTP + gRPC）](#6-第五层节点间通信http--grpc)
7. [第六层：一致性哈希](#7-第六层一致性哈希)
8. [第七层：Singleflight 防击穿](#8-第七层singleflight-防击穿)
9. [第八层：Protobuf 序列化](#9-第八层protobuf-序列化)
10. [第九层：服务发现与动态节点](#10-第九层服务发现与动态节点)
11. [第十层：熔断器 Circuit Breaker](#11-第十层熔断器-circuit-breaker)
12. [第十一层：指标埋点 / 健康检查 / 优雅关闭](#12-第十一层指标埋点--健康检查--优雅关闭)
13. [串联：一次 Get 请求的完整旅程](#13-串联一次-get-请求的完整旅程)
14. [设计模式与 Go 惯用法](#14-设计模式与-go-惯用法)

---

## 1. 从哪里开始？

这个项目的代码是**分层构建**的，每一层解决一个独立问题，组合起来构成完整的分布式缓存系统。推荐的阅读顺序（从底层数据结构到顶层分布式逻辑）：

```
lru.go → cache.go → byteview.go → geecache.go
       → peers.go → consistenthash.go → http.go → peers_grpc.go
       → singlefight.go → discovery.go → filediscovery.go
       → circuitbreaker.go → metrics.go
```

---

## 2. 第一层：LRU 缓存淘汰算法 + TTL 过期

**文件：** `lru/lru.go`

### 2.1 它解决什么问题？

当内存有限时，我们需要决定"淘汰谁"。LRU（Least Recently Used）的核心思想：**最近最少使用的数据最可能不再被需要**，优先淘汰它。此外，数据往往有时效性，需要支持**按时间过期**。

### 2.2 数据结构

```go
type Cache struct {
    maxBytes  int64                         // 最大允许使用的内存（0 表示不限制）
    nbytes    int64                         // 当前已使用内存
    ll        *list.List                    // Go 标准库双向链表，维护访问顺序
    cache     map[string]*list.Element      // key → 链表节点，O(1) 查找
    OnEvicted func(key string, value Value) // 淘汰回调（可选）
}

type entry struct {
    key       string
    value     Value
    expiresAt time.Time  // ★ 过期时间；零值 time.Time 表示永不过期
}
```

**为什么是"哈希表 + 双向链表"？**

- 哈希表提供 **O(1)** 的 Get
- 双向链表维护访问顺序：最近访问的移到头部，淘汰时从尾部移除
- `list.Element.Value` 存放 `*entry{key, value, expiresAt}`，这样淘汰时可以通过 key 删除 map 中的条目

### 2.3 核心操作

```
访问 key (Get):
  map[key] → 找到链表节点 → ★先判断是否过期（惰性过期，见下）→ MoveToFront（移到头部）→ 返回值
  未找到 → 返回 false

新增 key (Add(key, value, ttl)):
  已存在 → 更新 value + 过期时间 + 移到头部 + 调整内存计数
  不存在 → PushFront（插入头部）→ 写入 map → 调整内存计数
  如果内存超限 → 循环 RemoveOldest() 直到内存不超限

RemoveOldest:
  取链表尾部节点 → 从链表移除 → 从 map 删除 → 更新内存计数 → 执行回调
```

### 2.4 TTL 过期机制（惰性 + 主动）

`expiresAt` 在 `Add` 时设置：`ttl > 0` 才 `time.Now().Add(ttl)`，否则保持零值永不过期。过期回收用**双机制**：

**① 惰性过期（Get 时判断）：**

```go
func (c *Cache) Get(key string) (value Value, ok bool) {
    if ele, ok := c.cache[key]; ok {
        kv := ele.Value.(*entry)
        // ★ 命中后先判断是否过期
        if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
            c.removeElement(ele)  // 过期：删除 + 触发回调
            return nil, false
        }
        c.ll.MoveToFront(ele)
        return kv.value, true
    }
    return
}
```

**② 主动清理（CleanupExpired 全量扫描）：**

```go
func (c *Cache) CleanupExpired() int {
    count := 0
    // 从尾部向前遍历，先存 prev 再删，避免删除节点导致迭代器失效
    for e := c.ll.Back(); e != nil; {
        kv := e.Value.(*entry)
        prev := e.Prev()
        if !kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt) {
            c.removeElement(e)
            count++
        }
        e = prev
    }
    return count
}
```

**为什么需要双机制？** 只有惰性过期，长期不被访问的过期 key 会一直占内存；只有主动清理，清理间隔内仍可能读到过期数据（除非 Get 也判断）。两者互补：惰性保证正确性，主动保证及时释放内存。

**注意：** `CleanupExpired` **本身不加锁**，注释明确要求调用方（`cache` 层）先加锁再调用。这是锁层次设计的体现，见下一层。

### 2.5 关键细节

```go
// entry 同时存 key 和 value，因为淘汰时需要从 map 中删 key
// Value 接口：只要实现了 Len() 就能存入缓存
type Value interface {
    Len() int
}
```

**思考题：** 为什么 entry 要同时存 key？淘汰时直接用 `ll.Remove` 拿到 `*entry`，不就知道 key 了吗？—— 这正是答案：**淘汰/过期删除时需要从 `map` 中删除对应条目**，如果不存 key，就无法完成这个操作。

---

## 3. 第二层：并发安全的缓存封装

**文件：** `cache.go`

### 3.1 它解决什么问题？

`lru.Cache` 本身**不是并发安全**的。多个 goroutine 同时读写会引发 data race。`cache` 结构体在 LRU 外面**包了一层 `sync.Mutex`**。

### 3.2 设计

```go
type cache struct {
    mu         sync.Mutex
    lru        *lru.Cache
    cacheBytes int64    // 最大内存，用于延迟初始化
}

func (c *cache) add(key string, value ByteView, ttl time.Duration) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.lru == nil {                    // ★ 延迟初始化
        c.lru = lru.New(c.cacheBytes, func(key string, v lru.Value) {
            globalMetrics.RecordEviction()  // 淘汰时埋点
        })
    }
    c.lru.Add(key, value, ttl)
}
```

### 3.3 关键设计：延迟初始化（Lazy Initialization）

`c.lru == nil` 时才创建 —— LRU 对象不是创建 `cache` 时就初始化，而是**第一次 Add 时才创建**。这样如果一个 Group 从未被写入（比如只是查询），就不会分配 LRU 的内存。

### 3.4 后台清理 goroutine

```go
func (c *cache) StartCleanup(interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for range ticker.C {
            c.mu.Lock()
            if c.lru != nil {
                c.lru.CleanupExpired()  // 周期主动清理过期条目
            }
            c.mu.Unlock()
        }
    }()
}
```

**锁层次总结：** `cache.mu`（外层互斥锁）→ `lru.Cache`（内层，无锁）。因为 LRU 的 `Get` 也会 `MoveToFront` 改链表结构，读操作同样需要互斥，所以用 `Mutex` 而非 `RWMutex`。

---

## 4. 第三层：只读字节视图 ByteView

**文件：** `byteview.go`

### 4.1 它解决什么问题？

缓存中的值是 `[]byte`，但如果直接返回这个切片，调用方可能会意外修改它（Go 的切片共享底层数组）。`ByteView` 封装后，外部只能通过方法读取，无法修改。

### 4.2 实现

```go
type ByteView struct {
    b []byte  // 小写，包外不可直接访问
}

func (v ByteView) Len() int           { return len(v.b) }
func (v ByteView) ByteSlice() []byte  { return cloneBytes(v.b) }  // 返回副本
func (v ByteView) String() string     { return string(v.b) }

// 深拷贝
func cloneBytes(b []byte) []byte {
    c := make([]byte, len(b))
    copy(c, b)
    return c
}
```

**关键点：** `ByteSlice()` 返回的是**深拷贝**，不是原切片。虽然多了一次内存分配，但保证了缓存的不可变性。`Len()` 实现 `lru.Value` 接口，让 LRU 能统计占用内存。

---

## 5. 第四层：核心 Group —— 缓存命名空间

**文件：** `geecache.go`

### 5.1 它解决什么问题？

Group 是整个系统的**核心编排层**。它：

- 作为缓存命名空间（不同 Group 存不同数据，如 scores 和 users）
- 串联本地缓存、远程节点、本地数据源三者的查询逻辑
- 管理 Group 的全局注册表
- 内置 singleflight 防击穿和熔断器容错

### 5.2 结构

```go
type Group struct {
    name      string          // 命名空间，如 "scores"
    getter    Getter          // 缓存未命中时的回调：去哪加载原始数据
    mainCache cache           // 本地 LRU 缓存
    peers     PeerPicker      // 节点选择器（一致性哈希）
    loader    *singleflight.Group  // 合并并发请求
    DefaultTTL time.Duration      // ★ 默认过期时间，0 表示永不过期
    breaker   *CircuitBreaker     // ★ 远程节点熔断器
}
```

`NewGroup` 里固定初始化熔断器 `NewCircuitBreaker(3, 10*time.Second)`（连续失败 3 次熔断、10 秒后尝试恢复）。

### 5.3 Getter 接口 —— 函数式接口模式

```go
type Getter interface {
    Get(key string) ([]byte, error)
}

type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
    return f(key)  // 函数自己实现了接口！
}
```

这是一个经典的 Go 惯用法：**用函数类型实现接口**。使用时可以传一个匿名函数，非常便利：

```go
NewGroup("scores", 1<<20, GetterFunc(func(key string) ([]byte, error) {
    return db.Query(key), nil
}))
```

### 5.4 Group.Get() —— 核心查询流程

```go
func (g *Group) Get(key string) (ByteView, error) {
    if key == "" {
        return ByteView{}, fmt.Errorf("key is required")
    }
    // 第一步：查本地缓存
    if v, ok := g.mainCache.get(key); ok {
        globalMetrics.RecordHit()
        return v, nil
    }
    // 第二步：未命中，进入加载逻辑
    globalMetrics.RecordMiss()
    return g.load(key)
}
```

### 5.5 Group.load() —— 加载逻辑（含熔断降级）

```
load(key):
  ↓
singleflight.Do(key, fn)  ← 相同 key 只执行一次 fn
  ↓
fn 内部:
  ① 有 peers？→ 一致性哈希选节点 → 是远程节点？→ getFromPeer
     getFromPeer:
       - 熔断器打开？→ 快速失败，降级
       - peer.Get(req, res) 远程请求
       - 成功 → RecordSuccess + RecordPeerLoad
       - 失败 → RecordFailure → 降级
  ② 远程失败 / 本地 key → 降级到 getLocal
  ↓
getLocal(key):
  ① 调用 getter.Get(key)   ← 用户提供的回调，从 DB/文件加载
  ② 深拷贝数据 → 封装为 ByteView
  ③ 存入本地缓存（populateCache）
  ④ 返回 ByteView
```

**降级策略：** 如果远程节点请求失败（含熔断快速失败），不会直接返回错误，而是退回到本地数据源加载。这提高了系统的容错性。

### 5.6 设置 TTL

```go
// SetTTL 设置默认过期时间并启动后台清理
// cleanupInterval 为 0 时不启动后台清理（仅依赖 Get 时的惰性删除）
func (g *Group) SetTTL(ttl time.Duration, cleanupInterval time.Duration) {
    g.DefaultTTL = ttl
    if cleanupInterval > 0 {
        g.mainCache.StartCleanup(cleanupInterval)
    }
}
```

---

## 6. 第五层：节点间通信（HTTP + gRPC）

**文件：** `peers.go`、`http.go`、`peers_grpc.go`

### 6.1 接口解耦：PeerPicker / PeerGetter

节点间通信的核心是**两个接口**，把「选节点」和「取数据」抽象出来，让传输层可替换：

```go
// PeerPicker：用一致性哈希，找到 key 属于哪台服务器（挑选节点）
type PeerPicker interface {
    PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter：和选中的远程节点通信，获取缓存
type PeerGetter interface {
    Get(in *pb.Request, out *pb.Response) error
}
```

注意 `PeerGetter.Get` 已经是 **protobuf 风格**：把 group+key 装进 `Request`、结果写进 `Response`，序列化下沉到 getter 内部。上层 `Group.load` 完全不关心底层是 HTTP 还是 gRPC。

### 6.2 HTTP 通道（`http.go`）

`HTTPPool` 同时扮演两个角色：

- **服务端** `ServeHTTP`：实现 `http.Handler`，接收其他节点的查询请求。URL 格式 `/_geecache/<group>/<key>`。收到请求后 `GetGroup` + `group.Get(key)` 走完整链路，结果 `proto.Marshal` 成 `pb.Response` 写回。其中 `/health`、`/metrics` 两个路径免鉴权。
- **客户端** `httpGetter`：实现 `PeerGetter`，向其他节点发起 HTTP GET，读 body 后 `proto.Unmarshal` 到 `out`，内置 2 秒超时。

**节点管理：** `Set(peers...)` 建一致性哈希环 + 为每个非 self 对端建 `httpGetter`；`PickPeer(key)` 用哈希环找节点，返回对应的 getter。

**安全加固（可选）：**
- **Token 认证**：`SetSharedToken` + 请求头 `X-Geecache-Token` 校验，最简单内网认证。
- **TLS / mTLS**：`EnableTLS(certFile, keyFile, caFile)`，`caFile` 非空时开启双向 mTLS。

**编译期断言：**

```go
var _ PeerPicker = (*HTTPPool)(nil)  // 确保 HTTPPool 实现了 PeerPicker
var _ PeerGetter = (*httpGetter)(nil)
```

### 6.3 gRPC 通道（`peers_grpc.go`）

- **服务端 `GRPCServer`**：嵌入 `pb.UnimplementedGroupCacheServer`，实现 `Get(ctx, req)` —— 远程节点收到请求后 `GetGroup(req.Group)` + `group.Get(req.Key)`，把结果塞进 `pb.Response` 返回。
- **客户端 `grpcGetter`**：封装 `pb.GroupCacheClient`，把 gRPC 风格 `(ctx, in) → (*Resp, error)` 适配成 PeerGetter 的 `(in, out) → error`，2 秒超时。
- **`GRPCPool`**：`Set` 建哈希环 + 为每个对端 `grpc.NewClient` 建连接缓存；`PickPeer` 找节点取 getter；`Serve` 注册 service + listen；`Stop` 做 `GracefulStop`。

**HTTP 与 gRPC 如何共存：** 两者都实现 `PeerPicker` / `PeerGetter`，序列化统一用 protobuf，所以核心 `Group` 层无需感知差异。当前 `cmd/main.go` 示例主入口走 HTTP，gRPC 通道在 `peers_grpc_test.go` 里通过 `bufconn` 内存连接 + 真实端口做了完整测试。

---

## 7. 第六层：一致性哈希

**文件：** `consistenthash/consistenthash.go`

### 7.1 它解决什么问题？

普通哈希（如 `hash(key) % N`）在节点数量变化时，**几乎所有 key 的映射都会改变**。一致性哈希通过**哈希环 + 虚拟节点**，使得增减节点时只有少部分 key 需要重新分配。

### 7.2 核心概念

```
真实节点 A              → 虚拟节点 A#0, A#1, A#2, ... A#49  （50个）
每个虚拟节点计算哈希值，放在环上

查找 key 对应的节点：
  hash(key) → 在环上顺时针找到第一个 ≥ 该哈希的虚拟节点 → 返回对应的真实节点
```

### 7.3 实现细节

```go
type Map struct {
    hash     Hash           // 哈希函数（默认 CRC32）
    replicas int            // 每节点的虚拟节点数（默认 50）
    keys     []int          // 所有虚拟节点哈希，升序排列
    hashMap  map[int]string // 虚拟节点哈希 → 真实节点名
}

func (m *Map) Add(keys ...string) {
    for _, key := range keys {
        for i := 0; i < m.replicas; i++ {
            hashVal := int(m.hash([]byte(strconv.Itoa(i) + key)))
            m.keys = append(m.keys, hashVal)
            m.hashMap[hashVal] = key
        }
    }
    sort.Ints(m.keys)  // 排序，为二分查找做准备
}

func (m *Map) Get(key string) string {
    hashVal := int(m.hash([]byte(key)))
    idx := sort.Search(len(m.keys), func(i int) bool {
        return m.keys[i] >= hashVal  // 二分查找第一个 ≥ hashVal 的位置
    })
    return m.hashMap[m.keys[idx % len(m.keys)]]  // 环形取模
}
```

### 7.4 数据分布示例

```
虚拟节点数 = 3
节点 "6": 虚拟节点 06, 16, 26 → 哈希 6, 16, 26
节点 "4": 虚拟节点 04, 14, 24 → 哈希 4, 14, 24
节点 "2": 虚拟节点 02, 12, 22 → 哈希 2, 12, 22

哈希环排序: [2,4,6,12,14,16,22,24,26]

key "11" → hash=11 → 二分查找到 12 → 节点 "2"
key "27" → hash=27 → 超出所有 → 取模回到 2 → 节点 "2"
```

---

## 8. 第七层：Singleflight 防击穿

**文件：** `singleflight/singlefight.go`

### 8.1 它解决什么问题？

**缓存击穿场景：** 一个热点 key 过期，瞬间几百个并发请求同时发现缓存未命中，全部去查 DB，可能压垮数据库。

Singleflight 的思路：**对同一个 key，无论有多少并发请求，只执行一次加载操作**。其他请求等待这次操作完成，共享结果。

### 8.2 实现

```go
type call struct {
    wg  sync.WaitGroup  // 让后来的请求等待
    val interface{}
    err error
}

type Group struct {
    mu sync.Mutex
    m  map[string]*call  // key → 正在执行的请求
}

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
    g.mu.Lock()
    if c, ok := g.m[key]; ok {
        g.mu.Unlock()
        c.wg.Wait()     // ★ 已有请求在执行，等待它完成
        return c.val, c.err  // 共享结果
    }
    c := &call{}
    c.wg.Add(1)         // ★ 计数的是"正在执行的任务数"，恒为 1
    g.m[key] = c
    g.mu.Unlock()

    c.val, c.err = fn()  // 执行加载
    c.wg.Done()          // 通知所有等待者

    g.mu.Lock()
    delete(g.m, key)     // 清理，下次请求重新加载
    g.mu.Unlock()

    return c.val, c.err
}
```

### 8.3 执行时序

```
请求1: Do("Tom") → 创建 call → Add(1) → 执行 fn()
请求2: Do("Tom") → map 中已有 → Wait() 阻塞...
请求3: Do("Tom") → map 中已有 → Wait() 阻塞...
请求1: fn() 完成 → Done() → 请求2、3 同时被唤醒
请求2: 拿到 val, err → 返回
请求3: 拿到 val, err → 返回
```

---

## 9. 第八层：Protobuf 序列化

**文件：** `geecachepb/geecachepb.proto`

### 9.1 它解决什么问题？

节点间传输的是二进制数据。Protobuf 比 JSON 更紧凑，序列化/反序列化更快，适合缓存系统这种对性能敏感的场景。

### 9.2 Proto 定义

```protobuf
message Request {
    string group = 1;  // 哪个 Group
    string key = 2;    // 哪个 key
}

message Response {
    bytes value = 1;   // 缓存值（二进制）
}

service GroupCache {
    rpc Get(Request) returns (Response);  // gRPC 服务定义
}
```

`service GroupCache` 已经**不只是预留**了——`peers_grpc.go` 里 `GRPCServer`/`grpcGetter` 正是基于它生成的 `GroupCacheServer` / `GroupCacheClient` 实现 gRPC 通道。HTTP 通道则只用 `Request`/`Response` 两个 message 做序列化（`proto.Marshal` / `proto.Unmarshal`）。

---

## 10. 第九层：服务发现与动态节点

**文件：** `discovery.go`、`filediscovery.go`

### 10.1 它解决什么问题？

节点地址如果硬编码在 `main.go` 里，新增/下线节点需要重启所有服务。服务发现让节点列表可以**动态维护**。

### 10.2 接口抽象

```go
type ServiceDiscovery interface {
    GetPeers() ([]string, error)   // 获取所有节点地址
    Register(addr string) error    // 注册当前节点
    Watch(onChange func([]string)) // 监听节点变更
}
```

### 10.3 文件实现（FileDiscovery）

基于共享 JSON 文件 `peers.json`（无外部依赖，适合 demo）：

- `Register`：读文件 → 去重 → 把自己 merge 进去 → 写回。
- `Watch`：后台 goroutine 每 3 秒读一次文件，和上次列表对比，**变化时**回调 `onChange(peers)`；启动时先读一次并回调初始化。

### 10.4 节点变化如何驱动哈希环重建

`HTTPPool.StartDiscovery` 把两者接起来：

```go
func (p *HTTPPool) StartDiscovery(d ServiceDiscovery) error {
    if err := d.Register(p.self); err != nil { return err }
    d.Watch(func(peers []string) {
        p.Set(peers...)   // 节点变化 → 重建哈希环 + 重建 httpGetters
    })
    return nil
}
```

**抽象的意义：** 只要实现 `ServiceDiscovery` 接口，就可以把文件发现替换成 etcd / consul / zookeeper 等，而不用改 `Pool` 的代码。

---

## 11. 第十层：熔断器 Circuit Breaker

**文件：** `circuitbreaker.go`

### 11.1 它解决什么问题？

远程节点挂了，如果还不断去请求，会让调用方大量 goroutine 阻塞等待超时，进而拖垮上游（缓存雪崩的变体）。熔断器让故障节点**快速失败**，降级到本地回源。

### 11.2 三态状态机

```
Closed（闭合，正常放行）
   ↓ 连续失败达到阈值 N 次
Open（熔断打开，拒绝请求）
   ↓ 等待 timeout 时间到期
HalfOpen（半开，只放行探测请求）
   ├─ 探测请求成功 → 切回 Closed，重置失败计数
   └─ 探测请求失败 → 切回 Open，继续熔断
```

### 11.3 实现要点

```go
type CircuitBreaker struct {
    mu            sync.Mutex
    state         State  // Closed / Open / HalfOpen
    failureCount  int
    failureThresh int           // 连续失败多少次 → 熔断
    timeout       time.Duration // 熔断多久后尝试恢复
    lastFailure   time.Time
}
```

- **`IsOpen()`**：Closed→false；Open→若已过冷却期则切到 HalfOpen 并放行一个探测，否则拒绝；HalfOpen→放行探测。**半开是惰性触发的**——有请求来且已过冷却期才切。
- **`RecordSuccess()`**：直接复位 Closed + 清零计数。
- **`RecordFailure()`**：计数 +1；当 `failureCount >= threshold` 或当前是 HalfOpen 时切 Open。

### 11.4 接入点：getFromPeer

```go
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
    if g.breaker.IsOpen() {               // 熔断打开 → 快速失败
        return ByteView{}, ErrCircuitOpen
    }
    req := &pb.Request{Group: g.name, Key: key}
    res := &pb.Response{}
    if err := peer.Get(req, res); err != nil {
        g.breaker.RecordFailure()         // 远程失败 → 计数
        return ByteView{}, err
    }
    g.breaker.RecordSuccess()             // 成功 → 复位
    return ByteView{b: res.Value}, nil
}
```

`getFromPeer` 返回错误后，上层 `load` 自动降级到 `getLocal` 回源。本项目阈值/冷却固定为 `(3, 10s)`。

---

## 12. 第十一层：指标埋点 / 健康检查 / 优雅关闭

### 12.1 指标埋点（`metrics.go`）

```go
type Metrics struct {
    Hits       int64 // 命中次数
    Misses     int64 // 未命中次数
    LocalLoads int64 // 本地加载次数
    PeerLoads  int64 // 远程加载次数
    Evictions  int64 // 淘汰条目数
    TotalBytes int64 // 当前占用内存
}
```

- 全部用 `sync/atomic` 的 `AddInt64` / `LoadInt64`，**并发安全、无锁**。
- 命中率 = `hits / (hits + misses)`（分母 0 返回 0）。
- 埋点位置：命中/未命中在 `Get`；本地加载在 `getLocal`；远程加载在 `getFromPeer` 成功时；淘汰在 LRU 的 `OnEvicted` 回调。
- 通过 `/metrics` 端点以 Prometheus 文本格式暴露。

### 12.2 健康检查

`/health` 返回 `200 OK`，是最简存活探针（liveness），`run.sh` / `run.py` 用它轮询等节点就绪。

### 12.3 优雅关闭（`cmd/main.go`）

```go
quit := make(chan os.Signal, 1)
signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
<-quit                                   // 等退出信号

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
server.Shutdown(ctx)                     // 停收新连接，等现有请求完成
```

`http.Server.Shutdown` 与 `Close` 的区别：Shutdown 优雅（等请求处理完，可配超时），Close 立即断开所有连接。goroutine 里的 `ListenAndServe` 把 `http.ErrServerClosed` 当正常退出路径。

---

## 13. 串联：一次 Get 请求的完整旅程

以 `main.go` 中的示例为例，三个缓存节点 `:8001, :8002, :8003` + 一个 API 服务 `:9999`：

```
用户: curl "localhost:9999/api?key=Tom"
         │
         ▼
    API Server (:9999)
         │  group.Get("Tom")
         ▼
    [本地 :8003 的 mainCache.get("Tom")]
         │
         ├─ 命中？→ 直接返回
         │
         └─ 未命中 → load("Tom")
              │
              ▼
         singleflight.Do("Tom", fn)
              │  (相同 key 只执行一次)
              ▼
         fn:
           peers.PickPeer("Tom")
              │  一致性哈希: hash("Tom") → 在环上查 → 节点 :8002
              ▼
           getFromPeer(:8002, "Tom")
              │  ① breaker.IsOpen()? 熔断则降级
              │  ② HTTP GET http://localhost:8002/_geecache/scores/Tom
              │     （或 gRPC GroupCache.Get）
              ▼
         ┌──────────────────────────────────┐
         │  远程节点 :8002                    │
         │  ServeHTTP / GRPCServer.Get       │
         │  → group.Get("Tom")               │
         │  ① 查本地缓存 → miss              │
         │  ② load("Tom") → PickPeer → 本地 │
         │  ③ getLocal("Tom")               │
         │     → getter.Get("Tom")          │
         │     → 查 db 得 "630"             │
         │     → populateCache              │
         │     → 返回 "630"                 │
         └──────────────────────────────────┘
              │  pb.Response{Value: "630"}
              ▼
         :8003 收到响应
         populateCache → 本地也缓存 "630"
         返回 "630"
              │
              ▼
         用户收到: 630
```

**关键观察：**

1. **请求可能被路由到任意节点** —— 一致性哈希决定了 key 的归属
2. **数据会被缓存两层** —— 请求方节点和归属节点都会缓存
3. **Singleflight 在 load 入口** —— 保护了"远程请求"和"本地加载"两个路径
4. **远程失败自动降级** —— 远程挂了（或熔断）就本地加载，保证可用性
5. **熔断保护** —— 远端连续失败 3 次后 10 秒内跳过远程，快速降级

---

## 14. 设计模式与 Go 惯用法

### 14.1 函数式接口

```go
type GetterFunc func(key string) ([]byte, error)
func (f GetterFunc) Get(key string) ([]byte, error) { return f(key) }
```

让函数字面量实现接口，省去定义结构体。Go 标准库的 `http.HandlerFunc` 同理。

### 14.2 编译器接口断言

```go
var _ PeerPicker = (*HTTPPool)(nil)
var _ PeerGetter = (*httpGetter)(nil)
```

零成本确保类型实现了接口，比文档注释可靠得多。

### 14.3 延迟初始化

```go
if c.lru == nil {
    c.lru = lru.New(c.cacheBytes, nil)
}
```

不提前分配，用的时候再创建。`singleflight.Group` 的 map 也用了同样的策略。

### 14.4 只读视图

```go
type ByteView struct { b []byte }  // 小写字段，包外不可见
func (v ByteView) ByteSlice() []byte { return cloneBytes(v.b) }
```

通过封装 + 深拷贝保护内部状态，是防御性编程的典型实践。

### 14.5 接口隔离 + 依赖倒置

核心 `Group` 只依赖 `PeerPicker` / `PeerGetter` / `ServiceDiscovery` 等抽象接口，不依赖具体传输或注册中心实现，所以 HTTP 换 gRPC、文件发现换 etcd 都不用改核心逻辑。

### 14.6 分层解耦

每一层解决一个问题，通过接口连接：

```
lru.Cache  ←  cache  ←  Group  →  PeerPicker/PeerGetter  →  HTTPPool / GRPCPool  ←  consistenthash.Map
                ↑                    ↑                              ↑                        ↑
            缓存算法              节点路由 + 熔断                  网络通信                 一致性哈希
                                                                   ↑
                                                            ServiceDiscovery（节点拓扑）
```

替换任何一层都不影响其他层。

---

## 总结

这个项目的学习价值在于：

1. **经典缓存系统设计** —— 展示了一个分布式缓存从底层到顶层的完整架构
2. **Go 并发编程** —— sync.Mutex、sync.WaitGroup、sync/atomic、goroutine 的实际应用
3. **系统设计权衡** —— 一致性 vs 可用性（远程失败降级）、性能 vs 一致性（本地缓存 + 深拷贝）、惰性 vs 主动（TTL 过期）
4. **代码组织** —— 接口隔离、分层构建、编译期检查

建议按照本文的层级顺序阅读源码，每理解一层后再看下一层，效果最佳。
