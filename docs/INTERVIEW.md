# tutucache 面试通关手册

> 这是给项目作者本人的面试复习材料，不是 README。核心目标：把「读得懂」升级成「讲得出、推得动、扛得住追问」。
> 所有结论都对应真实代码（`file:line` 可点进去核对）。先看懂，再合上手册复述，最后做 mock 面试。

---

## Part 0 — 一页纸架构总览（先能徒手画出来）

### 0.1 完整请求链路（白板图）

```
                     ┌─────────────────────────────────────────────┐
                     │              Group（缓存命名空间）              │
                     │  name / getter / mainCache / loader / breaker │
                     └─────────────────────────────────────────────┘
                                        │
                              Group.Get(key)
                                        │
                          ┌─────────────┴──────────────┐
                          │   mainCache.get(key) 命中？  │── 是 → RecordHit → 返回 ByteView
                          └─────────────┬──────────────┘
                                        │ 否 → RecordMiss
                                        ▼
                              load(key)  ── singleflight.Do(key, fn)   ← 同 key 并发只执行一次
                                        │
                          ┌─────────────┴──────────────┐
                          │  peers != nil 且 PickPeer  │
                          │  命中某个远程节点？          │
                          └─────────────┬──────────────┘
                                        │ 是
                                        ▼
                          getFromPeer(peer)            ← breaker.IsOpen()? 熔断→快速失败
                          ┌─────────────┴──────────────┐
                          │  peer.Get(req, res) 成功？  │── 成功 → RecordSuccess + RecordPeerLoad → 返回
                          └─────────────┬──────────────┘
                                        │ 失败（含熔断 / 本地 key）
                                        ▼
                          getLocal(key)                 ← getter.Get 回源（DB/本地）
                                        │
                                        ▼
                          populateCache(key, value)     ← 写回本地 LRU
```

**一句话版本**：`Get` 先查本地 LRU，miss 后经 singleflight 合并，用一致性哈希选远程节点去取；远程取不到（或熔断）就降级回源，回源后写回本地缓存。

### 0.2 模块地图（横向一句话）

| 层次 | 模块 | 一句话职责 |
|---|---|---|
| 数据结构 | `ByteView` | 只读不可变字节，缓存值的统一形态 |
| 数据结构 | `lru.Cache` | LRU 淘汰 + TTL 过期（惰性+主动） |
| 并发封装 | `cache` | sync.Mutex 包住 lru，懒加载 + 后台清理 |
| 并发工具 | `singleflight` | 合并同 key 并发，防击穿 |
| 寻址 | `consistenthash.Map` | 一致性哈希 + 虚拟节点，选节点 |
| 解耦 | `PeerPicker/PeerGetter` | 接口抽象「找节点」与「取数据」 |
| 传输 | `HTTPPool` / `GRPCPool` | 两种通道，都实现上面接口，protobuf 序列化 |
| 拓扑 | `ServiceDiscovery` | 节点列表动态维护 → 驱动哈希环重建 |
| 保护 | `CircuitBreaker` | 三态熔断，快速失败降级回源 |
| 可观测 | `metrics` + `/health` | 命中率等指标 + 存活探针 |
| 工程化 | `graceful shutdown` | 信号监听 + http.Server.Shutdown |

### 0.3 两版自我介绍话术

**30 秒版（被问「先介绍下你的项目」）**：

> 这是一个用 Go 写的分布式内存缓存，参考 groupcache 的设计。核心是：单机用 LRU 加 TTL 过期做缓存，多机之间用一致性哈希加虚拟节点做 key 的路由，节点扩缩容时尽量少迁移；并发相同 key 的请求用 singleflight 合并，避免回源击穿；远程节点故障用熔断器快速失败并降级到本地回源，避免被故障节点拖垮。节点间通过 PeerPicker/PeerGetter 接口解耦传输层，我实现了 HTTP 和 gRPC 两种通道，序列化统一用 protobuf。另外补了指标埋点、健康检查和优雅关闭。

**3 分钟版（展开节奏，按这个顺序讲）**：

1. 需求：缓存数据，多机扩容，避免回源风暴 → 引出三个核心问题（穿透/击穿/雪崩）。
2. 单机：LRU + TTL，为什么 LRU（时间局部性）、TTL 用惰性+主动双机制。
3. 分布式：为什么不能简单取模 → 一致性哈希 + 虚拟节点。
4. 防击穿：singleflight；防雪崩：熔断降级。
5. 工程细节：接口解耦、protobuf、指标、健康检查、优雅关闭。

---

## Part 1 — 逐模块精讲（怎么实现 / 为什么 / 会被追问什么 / 标准答法）

> 顺序按「底层数据结构 → 上层编排」，理解依赖关系比背顺序重要。

### 1.1 ByteView — 不可变字节视图

**怎么实现**（`byteview.go:5-7`）：一个结构体，字段是私有的 `b []byte`，不对外暴露底层数组。三个方法：
- `Len()`：实现 `lru.Value` 接口，让 LRU 能统计它占多少内存。
- `ByteSlice()`：返回 `cloneBytes(v.b)` —— 深拷贝，不是直接返回底层切片。
- `String()`：调试用。

**为什么这么设计**：
1. **不可变**：缓存值是多个 goroutine 共享的，如果直接暴露 `[]byte`，调用方拿到引用后改了底层数组，缓存就被污染了。私有字段 + 读时深拷贝，保证「谁也别想改」。
2. **内存统计**：缓存有字节上限，LRU 淘汰要按大小算，`Len()` 让 ByteView 能参与这个统计。
3. **统一形态**：不管回源拿到的是字符串还是字节，都归一成 ByteView。

**会被追问**：
- 「为什么 `ByteSlice()` 要拷贝而不是直接返回？」—— 防外部篡改，破缓存不可变约定。
- 「拷贝的性能损耗呢？」—— 缓存命中本来就要把数据交给调用方，一次拷贝换来安全，且 Go 的 `copy` 很快；如果想零拷贝，需要调用方承诺只读，但 Go 没有 const 借用，接口约束不了。

**标准答法**：一句话「私有字段 + 读时深拷贝，实现不可变字节视图，既防篡改又让 LRU 能统计内存」。

---

### 1.2 lru.Cache — LRU 淘汰 + TTL 过期

**怎么实现**（`lru/lru.go:10-23`）：

```go
type Cache struct {
    maxBytes  int64
    nbytes    int64
    ll        *list.List               // 双向链表：Front=最近访问，Back=最久未访问
    cache     map[string]*list.Element // key → 链表节点，O(1) 查找
    OnEvicted func(key string, value Value)  // 淘汰回调
}
type entry struct {
    key       string
    value     Value
    expiresAt time.Time   // 过期时间；零值 = 永不过期
}
```

- **LRU 本体**：双向链表 + map。链表维护访问顺序，map 提供 O(1) 定位。`Get` 命中后 `MoveToFront` 把节点移到队首；`Add` 后若 `maxBytes < nbytes` 就从队尾 `RemoveOldest` 循环淘汰，直到不超限。
- **TTL 过期（你加的）**：时间戳 `expiresAt` 存在每个 `entry` 里。`Add` 时 `ttl > 0` 才设 `expiresAt = time.Now().Add(ttl)`，否则保持零值表示永不过期。
  - **惰性过期**：`Get` 命中后先判断 `!kv.expiresAt.IsZero() && time.Now().After(kv.expiresAt)`，过期则删节点、减 `nbytes`、触发 `OnEvicted`，返回未命中。
  - **主动清理**：`CleanupExpired()` 从队尾向前遍历，删掉所有过期条目。**它自己不加锁**，注释明确要求调用方先加锁。

**为什么这么设计**：
- LRU 用「链表 + map」是因为要同时做到「O(1) 查找」和「O(1) 调整顺序」。链表管顺序，map 管定位，二者通过 `*list.Element` 连接。
- TTL 用「惰性 + 主动」双机制：惰性保证读的时候绝不会返回过期数据（正确性）；主动清理避免过期条目长期占内存（内存友好）。两种只取其一都有缺陷——只有惰性，过期数据没人读就一直占内存；只有主动，清理间隔内可能读到过期数据（如果惰性不判断）。

**会被追问**：
- **「为什么 Get 要全程持锁？」** —— 这是最常考的并发点。见 1.3 的锁层次说明：`Get` 里的 `MoveToFront` 会改链表结构，如果读操作不加锁，两个 goroutine 并发 Get 会破坏链表指针，所以读也必须是互斥的。
- **「惰性过期和主动清理，只做一种行不行？」** —— 见上面「为什么」。
- **「过期时间和 LRU 淘汰谁先触发？」** —— Get 时惰性判断优先；主动清理独立触发；`RemoveOldest` 只按 LRU 顺序不管 TTL，但被 `CleanupExpired` 处理过的条目不会留到 `RemoveOldest`。
- **「为什么 expiresAt 放 entry 而不是单独的最小堆？」** —— 因为 TTL 主要是读时判断，放 entry 里 O(1)；单独最小堆适合「主动批量清理」场景，但本项目的主动清理是 O(n) 全扫，不需要堆。这是「根据访问模式选数据结构」的典型例子。

**标准答法**：LRU = 双向链表 + map（O(1) 查找 + O(1) 换序）；TTL = 时间戳存 entry，读时惰性判断 + 后台 ticker 主动清理，双机制互补。

---

### 1.3 cache — 并发安全封装（锁层次）

**怎么实现**（`cache.go:11-15`）：`cache` 结构体持有一把 `sync.Mutex`、一个 `*lru.Cache` 指针、一个 `cacheBytes`。`add`/`get`/`StartCleanup` 全都在 `Mutex` 保护下操作 lru。lru 首次 `add` 时懒加载（`lru.New(cacheBytes, ...)`），并把淘汰回调注册成 `globalMetrics.RecordEviction()`。

**锁层次（关键）**：`cache.mu`（外层 sync.Mutex）→ `lru.Cache`（内层，**本身无锁**）。lru 的所有方法都假设外部已加锁。

**为什么这么设计**：
- **为什么 lru 自己不加锁，而是外层加？** 因为 lru 的多个操作（比如 `Get` 后可能紧跟 `Add`）需要作为一个整体原子执行，锁加在内层会让「跨方法」的操作无法原子化，还容易死锁/重复锁。把锁放在 cache 层，lru 就变成一个「非线程安全的纯数据结构」，职责单一、好测试（测试里用无锁的 lru 也很方便）。
- **为什么用 `sync.Mutex` 而不是 `sync.RWMutex`？** 这是高频追问。因为 LRU 的**读操作也会改链表结构**（`Get` 命中要 `MoveToFront`），不是纯读，所以读锁没有意义，用互斥锁更简单正确。如果缓存是「读远多于写且读不改结构」的场景，才值得用 RWMutex。

**会被追问**：
- 「为什么 Get 这种读也要加写锁（Mutex）？」—— MoveToFront 改链表。
- 「懒加载 lru 有什么好处？」—— 空 Group 不占内存，直到第一次写才真正分配。

**标准答法**：锁放外层、lru 无锁，因为 LRU 读也改结构，用互斥锁；懒加载省内存。

---

### 1.4 singleflight — 合并并发同 key 请求

**怎么实现**（`singleflight/singlefight.go`）：`Group` 持 `mu sync.Mutex` + `m map[string]*call`。`call` 持 `wg sync.WaitGroup` + `val` + `err`。

`Do(key, fn)` 的流程：
1. 加锁，懒加载 map。
2. 如果 `m[key]` 已存在 → 解锁，`c.wait()` 阻塞等结果。
3. 否则新建 `call`，`c.wg.Add(1)`，写入 map，解锁。
4. 只有**第一个** goroutine 执行 `fn()`，结果写进 `c.val/c.err`，`c.wg.Done()`。
5. 执行完从 map 删掉 key，返回 `c.val/c.err`。

**为什么用 `wg.Add(1)` 而不是「按等待者计数」**：这是 singleflight 的经典正确用法。`WaitGroup` 在这里计数的是「正在执行的任务数」（恒为 1），而不是「等待者数」。执行者 `Done()` 一次，所有 `Wait()` 的 goroutine 一起被唤醒，拿到同一份 `val/err`。如果错误地给每个等待者 `Add(1)`，要么要精确配对每个 `Done()`，要么会漏唤醒。

**会被追问**：
- 「它解决的是哪个缓存问题？」—— 击穿。大量并发请求同一个 key，缓存又刚好过期/不存在，会同时穿透到 DB，singleflight 让同 key 只回源一次，其余等结果。
- 「如果 fn 执行很慢，其他请求会一直阻塞吗？」—— 会，这是它的代价。生产上通常给 fn 加超时（context），或者对「非常热且慢」的 key 单独处理。groupcache 的 singleflight 也面临同样问题。
- 「两个不同 key 会互相影响吗？」—— 不会，map 按 key 分桶。
- 「map 并发安全吗？」—— `mu` 保护，读写都加锁。

**标准答法**：用 map 记录「进行中的调用」，同 key 第一个请求执行回源，其余 `Wait` 复用结果，防击穿；WaitGroup 计数的是任务数（1），不是等待者数。

---

### 1.5 consistenthash.Map — 一致性哈希 + 虚拟节点

**怎么实现**（`consistenthash/consistenthash.go`）：

```go
type Map struct {
    hash     Hash           // 默认 crc32.ChecksumIEEE
    replicas int            // 每真实节点的虚拟节点数（项目里 = 50）
    keys     []int          // 所有虚拟节点哈希值，升序
    hashMap  map[int]string // 虚拟节点哈希 → 真实节点名
}
```

- `Add(keys...)`：对每个真实节点生成 `replicas` 个虚拟节点，标识为 `strconv.Itoa(i) + key`（i=0..replicas-1），算哈希塞进 `keys` 和 `hashMap`，最后 `sort.Ints` 升序。
- `Get(key)`：算 `key` 的哈希，`sort.Search` 二分找第一个 `>= hashVal` 的虚拟节点，`idx % len` 环形回绕，返回真实节点名。

**为什么用一致性哈希而不是简单取模**：
- 简单 `hash(key) % N` 在节点数 N 变化时，**几乎所有 key 的映射都会变**（因为模数变了），节点扩缩容会导致大面积缓存失效、迁移成本 O(N)。
- 一致性哈希把「节点」和「key」都映射到一个环上，节点变化时**只有相邻区间内的 key 需要迁移**，迁移量约 1/N，大幅减小。

**虚拟节点解决什么**：
- 真实节点数量少时，节点在环上分布不均匀，会出现「倾斜」——某些节点分到的 key 远多于其他，负载不均。
- 给每个真实节点加多个虚拟节点（如 50 个），让它们在环上更均匀地铺开，负载更均衡。虚拟节点越多，分布越均匀，但内存和查找成本也略增（keys 数组更长，二分查找 O(log n) 依然快）。

**会被追问**：
- 「crc32 为什么够用，不用 md5/sha？」—— 一致性哈希要的是「分布均匀」和「快」，不需要密码学安全，crc32 快且够均匀。
- 「`Add` 为什么不去重？重复 Add 会怎样？」—— 会累积重复虚拟节点，导致环冗余。项目里 `Pool.Set` 每次 `New` 一个新环再 `Add`，规避了重复累积。这是一个可以主动提的细节。
- 「二分查找 + 环形回绕怎么理解？」—— `keys` 是有序的哈希值环，找第一个 >= 目标哈希的位置，超出末尾就取 `%len` 回到队首，实现「环」。

**标准答法**：一致性哈希解决「节点数变化时大面积重映射」，虚拟节点解决「少量节点时分布倾斜」，查找用有序数组 + 二分 + 取模回绕。

---

### 1.6 PeerPicker / PeerGetter — 接口解耦传输层

**怎么实现**（`peers.go:9-16`）：

```go
type PeerPicker interface {
    PickPeer(key string) (peer PeerGetter, ok bool)  // 一致性哈希选节点
}
type PeerGetter interface {
    Get(in *pb.Request, out *pb.Response) error      // 与远程节点通信取数据
}
```

注意：这里 PeerGetter 的 `Get` 已经是 **protobuf 风格**（`in *pb.Request, out *pb.Response`），和原版 geecache 的 `Get(group, key string) ([]byte, error)` 不同——这是你「protobuf 序列化」落地的核心改动：把 group+key 装进 `Request`，结果写进 `Response`，序列化下沉到 getter 内部，上层 `Group.load` 完全不关心底层是 HTTP 还是 gRPC。

**为什么这么设计（精髓）**：
- 核心缓存逻辑（`Group`）只依赖两个接口，不依赖具体传输。换 HTTP → gRPC，`Group` 一行都不用改。这是「依赖倒置」：高层模块依赖抽象，不依赖具体实现。
- `HTTPPool` 和 `GRPCPool` 都实现 `PeerPicker`；`httpGetter` 和 `grpcGetter` 都实现 `PeerGetter`。编译期可用 `var _ PeerPicker = (*HTTPPool)(nil)` 断言。

**会被追问**：
- 「这个接口抽象带来什么好处？」—— 可替换传输层、可测试（测试里用 bufconn 起内存 gRPC server，见 `peers_grpc_test.go`）、职责清晰。
- 「为什么 PickPeer 和 Get 分开两个接口？」—— 「找谁」和「怎么取」是两件事：前者依赖一致性哈希，后者依赖具体 RPC。分开便于各自实现和替换。

**标准答法**：用两个接口把「选节点」和「取数据」抽象出来，Group 依赖抽象不依赖具体传输，所以 HTTP/gRPC 可无痛切换。

---

### 1.7 传输层：HTTPPool（http.go）与 GRPCPool（peers_grpc.go）

**HTTP 通道**（`http.go`）：
- `HTTPPool`：`self`（自己地址）、`basePath`（默认 `/_geecache/`）、`peers *consistenthash.Map`、`httpGetters map[string]*httpGetter`、可选 `tlsConfig` 和 `sharedToken`。
- 服务端 `ServeHTTP`：路由 `/_geecache/<group>/<key>`；`/health`、`/metrics` 免鉴权；其余校验 `X-Geecache-Token`；从 path 解析 group/key，`GetGroup` + `group.Get(key)` 走完整链路，结果 `proto.Marshal` 成 `pb.Response` 写回。
- 客户端 `httpGetter.Get`：拼 URL、GET、读 body、`proto.Unmarshal` 到 `out`，2 秒超时。

**gRPC 通道**（`peers_grpc.go`）：
- `GRPCServer`：嵌入 `pb.UnimplementedGroupCacheServer`，`Get(ctx, req)` 里 `GetGroup(req.Group)` + `group.Get(req.Key)`，把结果塞 `pb.Response`。
- `grpcGetter`：封装 `pb.GroupCacheClient`，把 gRPC 风格 `(ctx, in) → (*Resp, error)` 适配成 PeerGetter 的 `(in, out) → error`，2 秒超时，用 `proto.Merge` 拷贝（避免直接赋值触发 protobuf 内部 mutex 拷贝 panic）。
- `GRPCPool`：`Set` 建哈希环（50 虚拟节点）+ 为每个非 self 对端 `grpc.NewClient` 建连接缓存；`PickPeer` 找节点取 getter；`Serve` 注册 service + listen；`Stop` 做 `GracefulStop`。

**会被追问**：
- 「HTTP 和 gRPC 你是怎么做到共存的？」—— 都实现 PeerPicker/PeerGetter，序列化统一 protobuf，上层无感知。
- 「为什么 2 秒超时？」—— 避免远程节点故障时请求无限挂起；配合熔断器快速失败。
- 「TLS/token 是干什么的？」—— 节点间通信的加密（TLS/mTLS）和认证（共享 token），属于工程加固。
- 「`proto.Merge` 为什么要拷贝而不是 `*out = *resp`？」—— protobuf 生成的 struct 含内部 mutex，按值拷贝会触发 `go vet` 的 copylock 警告甚至运行时 panic，`proto.Merge` 是正确拷贝方式。这个细节能体现你踩过坑。

**标准答法**：两套传输共用一套接口和 protobuf 序列化；HTTP 用标准库手写路由 + proto.Marshal，gRPC 用生成的 client/server stub + 连接池复用。

---

### 1.8 服务发现 ServiceDiscovery + FileDiscovery

**怎么实现**：
- 接口（`discovery.go:3-10`）：`GetPeers()` / `Register(addr)` / `Watch(onChange func([]string))`。
- `FileDiscovery`（`filediscovery.go`）：基于共享 JSON 文件 `peers.json`（`["http://localhost:8001", ...]`），无外部依赖。
  - `Register`：读文件 → map 去重 → 把自己 merge 进去 → 写回。
  - `Watch`：后台 goroutine 每 3 秒 ticker 读一次文件，和上次列表做集合比较，**变化时**回调 `onChange(peers)`；启动时先读一次并回调初始化。
- 节点列表如何更新到哈希环：`HTTPPool.StartDiscovery` 里 `d.Register(p.self)` + `d.Watch(func(peers){ p.Set(peers...) })`，`Set` 内部重建哈希环和 getter map。

**为什么这么设计**：
- 抽象成 `ServiceDiscovery` 接口，就可以把「文件发现」替换成 etcd / consul / zookeeper 而不用改 `Pool`。这是接口解耦思想在拓扑层的复用。
- 文件轮询是最简单的实现（demo 够用），但生产会换成强一致或带 lease 的注册中心（节点挂了文件里还残留地址）。

**会被追问**：
- 「文件轮询有什么问题？」—— 节点崩溃不会从文件里移除（没有心跳/lease），会有「幽灵节点」；3 秒才有感知，非实时。
- 「为什么节点变化要重建整个哈希环而不是增量更新？」—— 简单正确优先；重建成本在节点规模小时可接受。

**标准答法**：接口抽象拓扑来源，文件轮询做 demo 版实现，变化时回调驱动哈希环重建；生产可替换成 etcd/consul。

---

### 1.9 CircuitBreaker — 三态熔断器

**怎么实现**（`circuitbreaker.go`）：

```
StateClosed ──连续失败≥阈值──▶ StateOpen ──冷却超时──▶ StateHalfOpen
    ▲                                                       │
    └────────────成功───────────────  ┌────失败（立刻重新 Open）
                                      │
                                   探测请求
```

- 状态机三态：`Closed`（正常放行）、`Open`（熔断，快速失败）、`HalfOpen`（放行一个探测请求）。
- `NewCircuitBreaker(3, 10*time.Second)`：**连续失败 3 次熔断，10 秒后尝试恢复**。
- `IsOpen()`：
  - Closed → false（放行）。
  - Open → 若 `time.Since(lastFailure) > timeout` 则切到 HalfOpen 并返回 false（放行一个探测），否则返回 true（拒绝）。
  - HalfOpen → false（放行探测）。
- `RecordSuccess()`：直接复位 `Closed` + 清零计数。
- `RecordFailure()`：`failureCount++` 记 `lastFailure`；当 `failureCount >= threshold` **或当前是 HalfOpen** 时，切 `Open`。

**半开是惰性触发的**：不是定时器主动切半开，而是「有请求来且已过冷却期」才切。半开只放一个探测，探测成功→Closed，探测失败→立刻重新 Open。

**降级接入点**：在 `Group.getFromPeer`。`IsOpen()` 为 true → 返回 `ErrCircuitOpen` 快速失败，上层 `load` 落到 `getLocal` 回源。远程请求成功/失败分别 `RecordSuccess`/`RecordFailure`。

**为什么这么设计**：
- 熔断器解决「雪崩」：一个下游节点故障，如果还不断去请求，会让调用方大量 goroutine 阻塞等待超时，进而拖垮上游。熔断让故障节点快速失败、跳过远程直接本地回源。
- 半开 + 冷却期是「自动恢复」的关键：不能一直熔断（下游可能已恢复），也不能立刻全量放行（可能还在故障），所以先放一个探测。

**会被追问**：
- **「熔断 vs 限流 vs 降级」区别** —— 限流是「控制进入系统的流量」；熔断是「下游不可用时快速失败」；降级是「提供兜底方案」。本项目：熔断触发后，降级动作是「本地回源」。
- **「你的熔断器是 per-group 还是 per-peer 粒度？」** —— 这是**主动加分点**（见 Part 4 雷点 4）：目前是 per-group，一个 group 一个 breaker，任一远端连续失败会熔断该 group 的所有远程。诚实说「demo 够用，生产要 per-peer」。
- 「阈值和冷却时间怎么定？」—— 阈值太敏感会误熔断，太迟钝起不到保护；冷却太短会反复抖动，太长影响可用性。实际靠压测调。

**标准答法**：三态状态机 + 半开探测自动恢复，配合降级回源，解决雪崩。

---

### 1.10 Group — 上层编排（把上面全串起来）

**怎么实现**（`geecache.go`）：`Group` 持 `name / getter / mainCache / peers / loader / DefaultTTL / breaker`。`Getter` 接口 `Get(key) ([]byte, error)` 是业务回源回调，`GetterFunc` 让普通函数适配成 Getter。

- `NewGroup`：nil getter panic；注册进全局 `groups` map（RWMutex 保护）；熔断器固定 `NewCircuitBreaker(3, 10s)`。
- `Get(key)`：空 key 报错 → `mainCache.get` 命中（RecordHit）→ miss（RecordMiss）→ `load`。
- `load(key)`：`loader.Do(key, fn)` 包住「远程取 → 失败降级 getLocal」，同 key 只执行一次。
- `getFromPeer`：熔断判断 → 远程 `peer.Get` → 记录成功/失败。
- `getLocal`：`getter.Get` 回源 → `cloneBytes` 深拷贝 → `populateCache` 写回。
- `SetTTL(ttl, cleanupInterval)`：设 DefaultTTL，cleanupInterval > 0 时启动后台清理。

**为什么回调用接口 + 函数适配器**：让使用者（业务方）不用实现结构体，直接传一个闭包就能定义「怎么回源」，Go 惯用法。

**会被追问**：
- 「远程节点递归查找会不会死循环？」—— 一致性哈希保证一个 key 只映射到一个节点，且 `PickPeer` 会跳过 self（`peer != self`），所以对端查到后要么命中要么自己回源，不会回头再来查本节点。不过要注意：**远程节点 miss 后是在对端自己回源**（对端也持有一份同样的 getter），而不是把请求又转发回来。
- 「一个 key 会不会同时被多个节点回源？」—— singleflight 只在单节点内合并；跨节点同 key 理论上可能各自回源（一致性哈希本应让同 key 只去一个节点，但节点列表不一致的窗口期可能有短暂双写）。这是分布式缓存一致性话题的引子。

**标准答法**：Group 是命名空间 + 编排者，把「本地缓存 → singleflight → 一致性哈希选节点 → 熔断降级 → 回源写回」串成一条链路。

---

### 1.11 指标埋点 + 健康检查 + 优雅关闭

**指标埋点**（`metrics.go`）：
- `Metrics{Hits, Misses, LocalLoads, PeerLoads, Evictions, TotalBytes}`，全部 `sync/atomic` 的 `AddInt64/LoadInt64/StoreInt64` 实现，**并发安全**。
- 命中率 = `hits / (hits + misses)`，分母 0 返回 0。
- 埋点位置：命中/未命中在 `Get`；本地加载在 `getLocal`；远程加载在 `getFromPeer` 成功时；淘汰在 `OnEvicted` 回调。
- 通过 `/metrics` 输出 Prometheus 文本格式。

**健康检查**：`/health` 返回 200 + "OK"，最简存活探针（liveness），免鉴权。

**优雅关闭**（`cmd/main.go:146-163`）：
- `signal.Notify(quit, SIGINT, SIGTERM)` 监听退出信号。
- `http.Server.Shutdown(ctx)`（10 秒超时）：先停止接受新连接，再等已建立连接的请求处理完。
- goroutine 里 `ListenAndServe` 把 `http.ErrServerClosed` 当正常退出。

**会被追问**：
- 「atomic 为什么比加锁好？」—— 计数场景原子操作更轻、无锁竞争；缺点是多个字段之间读不出「统一快照」（微弱不一致，监控场景可接受）。
- 「Shutdown 和 Close 区别？」—— Shutdown 优雅（等请求处理完），Close 立即（粗暴断开）。这也是高频 Go 题。
- 「health 是 liveness 还是 readiness？」—— 目前只有 liveness；生产还要 readiness（依赖检查）。

**标准答法**：atomic 无锁计数 + Prometheus 暴露；/health 存活探针；signal + Shutdown 优雅退出。

---

## Part 2 — 从 0 设计推导指南（补你最没底的地方）

> 核心心法：**每一步都是被「一个具体痛点」逼出来的**。面试时不要背「我要用 LRU、我要用一致性哈希」，而要讲「我遇到了 X 问题，所以用了 Y 方案，它解决了 X，代价是 Z」。下面给每一步的「痛点 → 方案 → 取舍」。

### 第 1 步：明确需求（先想清楚要什么，别急着写代码）
一个**分布式内存缓存**：
- 读多写少，数据来自慢速源（DB/本地文件），缓存起来加速。
- 内存有限，不能无限存 → 需要淘汰。
- 要能**多机水平扩容**，单机放不下或单机扛不住。

### 第 2 步：单机缓存 + 淘汰策略（为什么 LRU）
- 痛点：内存有限，满了要淘汰。
- 方案：先选淘汰策略。**LRU**（最近最少使用）契合「时间局部性」——刚访问过的很可能再访问。对比：FIFO 不考虑访问频率，可能淘汰热数据；LFU 考虑频率但实现更重、对新数据不友好（要老化处理）。
- 取舍：LRU 实现简单（链表+map）、够用，是缓存默认首选。
- 数据结构：**双向链表 + map**（O(1) 查找 + O(1) 换序）。

### 第 3 步：并发安全（锁怎么加）
- 痛点：Go 里缓存会被多个 goroutine 并发读写，map 并发写会 panic。
- 方案：加锁。锁的**粒度**是第一个设计点。
- 关键发现：LRU 的**读也会改链表**（MoveToFront），所以读不能只读锁，得互斥。→ 用 `sync.Mutex`，锁放外层（cache 层），lru 保持无锁的纯数据结构。
- 取舍：RWMutex 在这里无收益（读也要写链表）；锁粒度太细（lru 内层加锁）会让跨方法操作难原子化。

### 第 4 步：过期时间 TTL（惰性 vs 主动）
- 痛点：有些数据有时效性，不能一直缓存。
- 方案：给 entry 记 `expiresAt`。
  - **惰性过期**：读的时候判断，过期就删。保证正确性（读不到过期数据）。
  - **主动清理**：后台 ticker 定期扫，删过期条目。保证内存不泄漏。
- 取舍：只惰性 → 没人读的过期数据永远占内存；只主动 → 清理间隔内可能读到过期数据（除非也加惰性判断）。所以双机制。

### 第 5 步：拆分布式 + 一致性哈希（为什么不用取模）
- 痛点：单机内存/吞吐不够，要加机器。
- 方案 A：`hash(key) % N`。**痛点**：N 变了（加机器/减机器），模数变，几乎所有 key 都要重映射 → 缓存大面积失效，回源风暴。
- 方案 B：**一致性哈希**。节点和 key 都映射到哈希环，节点变化只影响相邻区间，迁移量约 1/N。
- **虚拟节点**：真实节点少时环上分布不均（倾斜），加虚拟节点（如 50 个）让分布均匀。
- 取舍：一致性哈希不是严格均匀，虚拟节点越多越均匀但内存/查找成本略增。

### 第 6 步：防击穿（singleflight）
- 痛点：热点 key 缓存过期的一瞬间，海量并发请求同时穿透到 DB，打挂 DB。
- 方案：singleflight，同 key 的并发请求合并成一次回源，其余等结果。
- 取舍：慢回源会拖住所有等待者，需配合超时。

### 第 7 步：防雪崩（熔断降级）
- 痛点：某个下游节点故障，若还不断请求，会让调用方大量 goroutine 阻塞等超时，拖垮上游，连锁反应。
- 方案：**熔断器**——连续失败 N 次后熔断，快速失败；冷却期后放一个探测（半开），成功恢复，失败继续熔断。熔断后的**降级**动作是「跳过远程，本地回源」。
- 取舍：粒度（per-group vs per-peer）、阈值、冷却时间都要调。

### 第 8 步：可观测性（指标 + 健康检查）
- 痛点：上线后看不到命中率、看不到哪一步是瓶颈、故障难定位。
- 方案：埋点（hits/misses/local_loads/peer_loads/evictions）+ 命中率 + `/metrics`（Prometheus）+ `/health`。
- 取舍：计数用 atomic 无锁，换「微弱不一致」换「无锁高性能」。

### 第 9 步：工程化（优雅关闭）
- 痛点：直接 kill 进程会丢正在处理的请求。
- 方案：监听 SIGINT/SIGTERM → `http.Server.Shutdown` 等请求处理完再退，超时兜底。

**一句话串联（面试万能开场）**：先单机 LRU+TTL 解决「存什么、存多久」，再一致性哈希解决「多机怎么分」，singleflight 防击穿、熔断降级防雪崩，最后补可观测和优雅关闭。每一步都是被一个具体痛点逼出来的。

---

## Part 3 — 高频面试题 Q&A（按主题）

### 缓存三大问题
**Q：缓存穿透、击穿、雪崩分别是什么？怎么解决？**
- **穿透**：查一个**根本不存在的 key**，缓存永远 miss，每次都打到 DB。解决：缓存空值（给不存在的 key 也缓存一个空标记 + 短 TTL）、布隆过滤器前置拦截、参数校验。
- **击穿**：一个**热点 key 恰好过期**，海量并发同时回源打 DB。解决：**singleflight 合并**（本项目）、热点 key 永不过期、加互斥锁。
- **雪崩**：**大量 key 同时过期** 或 **某个下游节点故障**，导致大面积回源/请求堆积。解决：TTL 加随机抖动避免同时过期、**熔断降级**（本项目）、多级缓存、限流。

**Q：你的项目分别对应了哪几个？**
- 击穿 → singleflight；雪崩（下游故障）→ 熔断降级；穿透 → 本项目没专门处理（可主动说这是「没做的点」，见 Part 4）。

### 一致性哈希
**Q：为什么不用 `hash(key) % N`？**
- N 变化时几乎所有 key 重映射，缓存失效风暴、迁移成本高。一致性哈希把迁移量降到约 1/N。

**Q：虚拟节点有什么用？**
- 真实节点少时环上分布倾斜，负载不均；虚拟节点让分布更均匀。数量越多越均匀，但查找/内存成本略增。

**Q：怎么实现的？**
- crc32 哈希，有序数组 + 二分找第一个 >= 目标哈希的位置，取模回绕成环。

### singleflight
**Q：singleflight 原理？**
- map 记录进行中的调用，同 key 第一个执行，其余 Wait 复用结果。WaitGroup 计数的是任务数（1）而非等待者数。

**Q：有什么坑？**
- 慢回源拖住所有等待者；返回的结果是共享的，调用方不能改（所以本项目用 ByteView 深拷贝）。

### 熔断器
**Q：熔断器状态机？**
- Closed → Open（连续失败达阈值）→ HalfOpen（冷却期后放一个探测）→ Closed（成功）/ Open（失败）。

**Q：熔断、限流、降级的区别？**
- 限流控流量，熔断对故障快速失败，降级给兜底。本项目熔断触发后降级到本地回源。

### LRU 与淘汰策略
**Q：LRU / LFU / FIFO 区别与适用？**
- LRU 按「最近访问时间」，契合时间局部性，缓存默认首选；LFU 按「访问频率」，适合热点稳定但实现重、需老化；FIFO 按「先进先出」，简单但不考虑访问模式。

**Q：LRU 为什么用链表 + map？复杂度？**
- 链表管顺序（O(1) 换序），map 管定位（O(1) 查找），通过 `*list.Element` 连接。Get/Set 均摊 O(1)。

### Go 并发
**Q：这里为什么用 Mutex 不用 RWMutex？**
- LRU 读也会 MoveToFront 改链表，不是纯读，RWMutex 无收益，Mutex 更简单正确。

**Q：atomic 和 Mutex 怎么选？**
- 简单计数用 atomic（无锁、更快）；多字段需要原子一致性、或保护一段复合逻辑时用 Mutex。

**Q：什么是 goroutine 泄漏？你项目里有吗？**
- goroutine 无法退出、一直占资源。本项目里文件发现轮询、TTL 清理 goroutine 没纳入优雅关闭（进程退出即终止），严格说生命周期没闭环——这是个可以主动说的改进点。

**Q：http.Server.Shutdown 和 Close 区别？**
- Shutdown 优雅（停收新连接、等请求处理完、可配超时）；Close 立即关闭所有连接。

### 分布式缓存通用
**Q：你的项目跟 Redis 有什么区别？**
- Redis 是独立进程的中心化缓存服务，网络访问、支持丰富数据结构、持久化；本项目是**内嵌库**（进程内缓存），像 groupcache，无中心节点、去中心化、适合读多写少、数据不需强一致的场景，延迟更低（无网络往返，本地查）。

**Q：缓存和 DB 的一致性问题？**
- 缓存是 DB 的副本，天然存在不一致窗口。本项目是读缓存 + 回源写回，不主动更新，靠 TTL 兜底最终一致。如果要强一致，需要写时失效/双删等策略。这是「本项目不解决写一致」的诚实边界。

**Q：节点宕机了怎么办？**
- 一致性哈希会自动把该节点的 key 重新映射到环上下一个节点；但会有短暂 miss + 回源。本项目熔断器会在故障期间降级回源，避免被拖垮。

---

## Part 4 — 简历对齐 + 雷点排查（主动讲，别被动翻车）

> 这一部分是你的「护城河」：**主动说出你项目里不完美的地方 + 你准备怎么改**，比被面试官追问到墙角强一百倍。每个雷点给三件套：「面试官可能怎么问」→「诚实且加分的说法」→「要不要补、怎么补」。

### 雷点 1（最高优先级）：简历写「Protobuf + gRPC」，但 main 跑的是 HTTP

- **事实**：`cmd/main.go:36` 用 `NewHTTPPool`，demo 走 HTTP；`GRPCPool` 只在 `peers_grpc_test.go` 里接线跑通。protobuf 序列化是真的（HTTP 通道也在用 `pb.Request/Response`）。
- **面试官可能问**：「你 demo 跑起来用的什么协议？」「gRPC 通道上线了吗？」
- **诚实且加分的说法**：> 「我实现了 HTTP 和 gRPC 两种传输，通过 PeerPicker/PeerGetter 接口解耦，序列化统一用 protobuf。demo 主入口当前走 HTTP，gRPC 通道我写了完整的单测和 bufconn 集成测试覆盖。之所以能这么做，正是接口解耦的好处——切通道不改核心逻辑。」
- **要不要补**：如果想让简历 bullet 100% 硬，可以把 main 切到 gRPC（给 `GRPCPool` 补 `StartDiscovery`、`startCacheServer` 换成 `NewGRPCPool`+`Serve()`）。**属于可选的代码增强，我可以帮你做**。不改代码的话，上面这个说法也完全站得住，且显得你理解双通道设计。

### 雷点 2：`TotalBytes` 指标声明了但没接线

- **事实**：`metrics.go` 有 `TotalBytes` 字段和 `SetTotalBytes`，但全项目无人调用，`cache_bytes` 恒为 0。
- **面试官可能问**：「你的 cache_bytes 指标为什么一直是 0？」
- **诚实说法**：> 「我声明了当前内存占用这个 gauge，但还没在 LRU 增删的路径上把它和实际字节数接通，所以现在恒为 0。这是我 TODO 列表里的一条——只需要在 `cache.add`/lru 淘汰/过期时调 `SetTotalBytes` 同步 `nbytes` 就行。」
- **怎么补**：在 lru 的 `nbytes` 变化处（Add/RemoveOldest/过期删除）回写 `metrics.SetTotalBytes`。改动很小，收益是让「内存占用」指标真实可用。

### 雷点 3：后台 goroutine 没纳入优雅关闭

- **事实**：`FileDiscovery` 轮询 goroutine（`filediscovery.go:70-98`）和 TTL 清理 goroutine（`cache.go:48-63`）在进程退出时只是随进程终止，没显式 `Close`。HTTP server 的优雅关闭是规范的，但这两个后台任务生命周期没闭环。
- **面试官可能问**：「你的优雅关闭完整吗？后台 goroutine 怎么退出的？」
- **诚实说法**：> 「HTTP 层我用 `signal.Notify` + `Shutdown` 做了优雅关闭，但文件发现的 3 秒轮询和 TTL 清理这两个后台 goroutine 目前是随进程退出的，没显式停。更完整的做法是用 context/stopCh 通知它们退出，统一纳入 shutdown 流程。」
- **怎么补**：`FileDiscovery` 已有 `Close()`（`close(stopCh)`），main 里补一句 `discovery.Close()`；TTL 清理用 `context` 或 `stopCh` 让 ticker goroutine 可退出。这个改进能直接讲出「goroutine 泄漏/生命周期」的知识点。

### 雷点 4：熔断器是 per-group 粒度

- **事实**：`geecache.go:34` 一个 Group 一个 breaker，任一远端连续失败 3 次会熔断该 group 的**所有**远程获取，10 秒内全降级本地。
- **面试官可能问**：「如果两个远程节点一个好一个坏，你的熔断器会怎样？」
- **诚实说法**：> 「目前熔断器是 group 级别的，一个 group 共享一个 breaker，所以某个远端连续失败会影响到这个 group 的所有远程请求。对只有单一『远端』语义的 demo 够用，但多异构 peer 场景粒度偏粗，生产化应该改成 per-peer 粒度，每个对端独立熔断。」
- **怎么补**：把 breaker 从 Group 移到 `PeerGetter` 级别（或 `map[peer]*CircuitBreaker`），每个对端独立计数。这是「熔断粒度」的经典设计权衡，讲出来很加分。

### 雷点 5：`GRPCPool` 没有 `StartDiscovery`

- **事实**：服务发现只接在 `HTTPPool` 上（`http.go:170-179`），`GRPCPool` 是静态 `Set` 拓扑。
- **面试官可能问**：「gRPC 通道下节点变化怎么办？」
- **诚实说法**：> 「服务发现目前只接在 HTTPPool 上，gRPC 通道是静态 Set 的。如果要把 gRPC 也做成动态拓扑，需要给 GRPCPool 也实现一个 StartDiscovery，复用同一个 ServiceDiscovery 接口，节点变化时重建连接和哈希环。」
- **怎么补**：给 `GRPCPool` 加 `StartDiscovery`（参考 `HTTPPool.StartDiscovery`），节点变化时 `Set(peers...)` 重建 `grpc.NewClient` 连接和哈希环。和雷点 1 是同一件事的两面——都在「gRPC 半成品」这个主题下。

### 汇总：一个「我主动交代的改进清单」话术

> 「如果要把这个项目生产化，我列了几个明确的改进点：① 把 gRPC 通道接进主入口并补上服务发现，让双通道都真正可用；② 把 TotalBytes 指标接线，让内存占用可观测；③ 让后台 goroutine 纳入优雅关闭生命周期；④ 熔断器从 per-group 细化到 per-peer。这些我都有清晰的改法，只是 demo 阶段优先保核心链路正确。」

这样一段话，比「我抄了个 geecache」高好几个段位——它证明你**理解了自己代码的边界**，这正是「能读代码」和「能设计系统」之间的那道坎。

---

## 附：练习清单（怎么练到有把握）

1. **白板**：只拿 Part 0 的链路图，合上手册，讲 30 秒版 + 3 分钟版。
2. **从 0 推导**：合上 Part 2，从「需求」推到「优雅关闭」，卡壳处回到对应「痛点→方案→取舍」。
3. **mock 面试**：让我扮演面试官逐题问（Part 3 + Part 4 的追问），你答完我给你打分纠错，反复几轮。
4. **跑起来自证**：`./run.sh` 起 3 节点，`curl` 验证命中/回源/熔断/`/health`/`/metrics` 每条路径，确保你讲的每个机制都能当场演示、每个指标都对应真实行为。
