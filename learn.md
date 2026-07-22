# tutucache 源码学习指南

> 本文带你从零开始，逐步串联这个分布式缓存项目，理解每一层的设计动机与实现细节。

## 目录

1. [从哪里开始？](#1-从哪里开始)
2. [第一层：LRU 缓存淘汰算法](#2-第一层lru-缓存淘汰算法)
3. [第二层：并发安全的缓存封装](#3-第二层并发安全的缓存封装)
4. [第三层：只读字节视图 ByteView](#4-第三层只读字节视图-byteview)
5. [第四层：核心 Group —— 缓存命名空间](#5-第四层核心-group--缓存命名空间)
6. [第五层：HTTP 节点间通信](#6-第五层http-节点间通信)
7. [第六层：一致性哈希](#7-第六层一致性哈希)
8. [第七层：Singleflight 防击穿](#8-第七层singleflight-防击穿)
9. [第八层：Protobuf 序列化](#9-第八层protobuf-序列化)
10. [串联：一次 Get 请求的完整旅程](#10-串联一次-get-请求的完整旅程)
11. [设计模式与 Go 惯用法](#11-设计模式与-go-惯用法)

---

## 1. 从哪里开始？

这个项目的代码是**分层构建**的，每一层解决一个独立问题，组合起来构成完整的分布式缓存系统。推荐的阅读顺序：

```
lru.go → cache.go → byteview.go → geecache.go → peers.go → consistenthash.go → http.go → singleflight.go
```

这正好是从**底层数据结构**到**顶层分布式逻辑**的自然演进。

---

## 2. 第一层：LRU 缓存淘汰算法

**文件：** `lru/lru.go`

### 2.1 它解决什么问题？

当内存有限时，我们需要决定"淘汰谁"。LRU（Least Recently Used）的核心思想：**最近最少使用的数据最可能不再被需要**，优先淘汰它。

### 2.2 数据结构

```go
type Cache struct {
    maxBytes  int64                         // 最大允许使用的内存
    nbytes    int64                         // 当前已使用内存
    ll        *list.List                    // Go 标准库双向链表，维护访问顺序
    cache     map[string]*list.Element      // key → 链表节点，O(1) 查找
    OnEvicted func(key string, value Value) // 淘汰回调（可选）
}
```

**为什么是"哈希表 + 双向链表"？**

- 哈希表提供 **O(1)** 的 Get
- 双向链表维护访问顺序：最近访问的移到头部，淘汰时从尾部移除
- `list.Element.Value` 存放 `*entry{key, value}`，这样淘汰时可以通过 key 删除 map 中的条目

### 2.3 核心操作

```
访问 key:
  map[key] → 找到链表节点 → MoveToFront（移到头部）→ 返回值
  未找到 → 返回 false

新增 key:
  已存在 → 更新 value + 移到头部 + 调整内存计数
  不存在 → PushFront（插入头部）→ 写入 map → 调整内存计数
  如果内存超限 → 循环 RemoveOldest() 直到内存不超限

RemoveOldest:
  取链表尾部节点 → 从链表移除 → 从 map 删除 → 更新内存计数 → 执行回调
```

### 2.4 关键细节

```go
// entry 同时存 key 和 value，因为淘汰时需要从 map 中删 key
type entry struct {
    key   string
    value Value
}

// Value 接口：只要实现了 Len() 就能存入缓存
type Value interface {
    Len() int
}
```

**思考题：** 为什么 entry 要同时存 key？淘汰时直接用 `ll.Remove` 拿到 `*entry`，不就知道 key 了吗？—— 这正是答案：**淘汰时需要通过 key 从 `map` 中删除对应条目**，如果不存 key，就无法完成这个操作。

---

## 3. 第二层：并发安全的缓存封装

**文件：** `cache.go`

### 3.1 它解决什么问题？

`lru.Cache` 本身不是并发安全的。多个 goroutine 同时读写会引发 data race。`cache` 结构体在 LRU 外面**包了一层 `sync.Mutex`**。

### 3.2 设计

```go
type cache struct {
    mu         sync.Mutex
    lru        *lru.Cache
    cacheBytes int64    // 最大内存，用于延迟初始化
}

func (c *cache) add(key string, value ByteView) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.lru == nil {                    // ★ 延迟初始化
        c.lru = lru.New(c.cacheBytes, nil)
    }
    c.lru.Add(key, value)
}
```

### 3.3 关键设计：延迟初始化（Lazy Initialization）

注意 `c.lru == nil` 的判断 —— LRU 对象不是创建 `cache` 时就初始化，而是**第一次 Add 时才创建**。这样做的好处是：如果某个 Group 从未被写入（比如只是查询），就不会分配 LRU 的内存。

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

**关键点：** `ByteSlice()` 返回的是**深拷贝**，不是原切片。虽然多了一次内存分配，但保证了缓存的不可变性。

---

## 5. 第四层：核心 Group —— 缓存命名空间

**文件：** `geecache.go`

### 5.1 它解决什么问题？

Group 是整个系统的**核心编排层**。它：

- 作为缓存命名空间（不同 Group 存不同数据，如 scores 和 users）
- 串联本地缓存、远程节点、本地数据源三者的查询逻辑
- 管理 Group 的全局注册表

### 5.2 结构

```go
type Group struct {
    name      string          // 命名空间，如 "scores"
    getter    Getter          // 缓存未命中时的回调：去哪加载原始数据
    mainCache cache           // 本地 LRU 缓存
    peers     PeerPicker      // 节点选择器（一致性哈希）
    loader    *singleflight.Group  // 合并并发请求
}
```

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
        return v, nil
    }
    // 第二步：未命中，进入加载逻辑
    return g.load(key)
}
```

### 5.5 Group.load() —— 加载逻辑

```
load(key):
  ↓
singleflight.Do(key, fn)  ← 相同 key 只执行一次 fn
  ↓
fn 内部:
  ① 有 peers？→ 一致性哈希选节点 → 是远程节点？→ HTTP 请求远程
    远程失败？→ 降级到本地
  ② 没有 peers？→ 直接走本地
  ↓
getLocal(key):
  ① 调用 getter.Get(key)   ← 用户提供的回调，从 DB/文件加载
  ② 深拷贝数据 → 封装为 ByteView
  ③ 存入本地缓存
  ④ 返回 ByteView
```

**降级策略：** 如果远程节点请求失败，不会直接返回错误，而是退回到本地数据源加载。这提高了系统的容错性。

---

## 6. 第五层：HTTP 节点间通信

**文件：** `http.go`

### 6.1 它解决什么问题？

多个缓存节点之间需要通信。`HTTPPool` 同时扮演两个角色：

- **服务端**：实现 `http.Handler`，接收其他节点的缓存查询请求
- **客户端**：通过 `httpGetter` 向其他节点发起 HTTP 请求

### 6.2 服务端：ServeHTTP

```go
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // URL 格式: /<basepath>/<groupname>/<key>
    // 例如: /_geecache/scores/Tom
    parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
    groupName := parts[0]
    key := parts[1]

    group := GetGroup(groupName)  // 从全局注册表拿 Group
    view, err := group.Get(key)   // 走正常的缓存查询流程
    // 序列化为 protobuf 返回
    body, _ := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
    w.Write(body)
}
```

**注意：** 收到远程请求的节点，自己也会走 `group.Get(key)`。如果它本地也没有，会降级到 `getLocal()` 加载数据。

### 6.3 客户端：httpGetter

```go
type httpGetter struct {
    baseURL string  // "http://localhost:8001/_geecache/"
}

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
    u := fmt.Sprintf("%v%v/%v", h.baseURL, in.GetGroup(), in.GetKey())
    res, err := http.Get(u)
    // ... 读取 body，proto.Unmarshal 到 out
}
```

### 6.4 接口与编译期断言

```go
// 编译期检查：确保 HTTPPool 实现了 PeerPicker
var _ PeerPicker = (*HTTPPool)(nil)

// 编译期检查：确保 httpGetter 实现了 PeerGetter
var _ PeerGetter = (*httpGetter)(nil)
```

这是一个常用的 Go 技巧：如果类型没有实现接口，**编译就会报错**，而不是等到运行时才发现。

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
    c.wg.Add(1)
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
    rpc Get(Request) returns (Response);  // 预留给 gRPC 扩展
}
```

---

## 10. 串联：一次 Get 请求的完整旅程

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
              │  HTTP GET http://localhost:8002/_geecache/scores/Tom
              ▼
         ┌──────────────────────────────────┐
         │  远程节点 :8002                    │
         │  ServeHTTP → group.Get("Tom")     │
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
4. **远程失败自动降级** —— 远程挂了就本地加载，保证可用性

---

## 11. 设计模式与 Go 惯用法

### 11.1 函数式接口

```go
type GetterFunc func(key string) ([]byte, error)
func (f GetterFunc) Get(key string) ([]byte, error) { return f(key) }
```

让函数字面量实现接口，省去定义结构体。Go 标准库的 `http.HandlerFunc` 同理。

### 11.2 编译器接口断言

```go
var _ PeerPicker = (*HTTPPool)(nil)
```

零成本确保类型实现了接口，比文档注释可靠得多。

### 11.3 延迟初始化

```go
if c.lru == nil {
    c.lru = lru.New(c.cacheBytes, nil)
}
```

不提前分配，用的时候再创建。`singleflight.Group` 也用了同样的策略。

### 11.4 只读视图

```go
type ByteView struct { b []byte }  // 小写字段，包外不可见
func (v ByteView) ByteSlice() []byte { return cloneBytes(v.b) }
```

通过封装 + 深拷贝保护内部状态，是防御性编程的典型实践。

### 11.5 分层解耦

每一层解决一个问题，通过接口连接：

```
lru.Cache  ←  cache  ←  Group  →  PeerPicker/PeerGetter  →  HTTPPool  ←  consistenthash.Map
                ↑                    ↑                            ↑
            缓存算法              节点路由                     网络通信
```

替换任何一层都不影响其他层。比如可以把 HTTP 换成 gRPC，只需实现 `PeerGetter` 接口即可。

---

## 总结

这个项目的学习价值在于：

1. **经典缓存系统设计** —— 展示了一个分布式缓存从底层到顶层的完整架构
2. **Go 并发编程** —— sync.Mutex、sync.WaitGroup、goroutine 的实际应用
3. **系统设计权衡** —— 一致性 vs 可用性（远程失败降级）、性能 vs 一致性（本地缓存 + 深拷贝）
4. **代码组织** —— 接口隔离、分层构建、编译期检查

建议按照本文的层级顺序阅读源码，每理解一层后再看下一层，效果最佳。
