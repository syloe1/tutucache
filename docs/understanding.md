# 我对 tutucache 的理解（个人笔记）

> 一句话：一个 Go 写的**分布式内存缓存**——单机用 LRU+TTL 做缓存，多机用一致性哈希路由 key，singleflight 防击穿、熔断器容错降级，节点间 HTTP/gRPC 双传输 + protobuf 序列化。

## 一、一条 Get 请求的完整链路

```
Group.Get(key)
  ├─ 查本地 LRU 缓存 mainCache.get(key)
  │     ├─ 命中 → RecordHit → 返回 ByteView
  │     └─ 未命中 → RecordMiss → 进 load
  └─ load(key) = singleflight.Do(key, fn)   // 同 key 并发只执行一次
        └─ fn 内部：
             ├─ 有 peers？→ 一致性哈希 PickPeer(key) 选远程节点
             │     └─ getFromPeer(peer)
             │           ├─ 熔断器打开？→ 快速失败（ErrCircuitOpen）
             │           ├─ peer.Get(req, res) 远程取
             │           │     ├─ 成功 → RecordSuccess + RecordPeerLoad → 返回
             │           │     └─ 失败 → RecordFailure → 降级
             │           └─ 降级 → getLocal(key)
             └─ 本地 key / 远程失败 → getLocal(key)
                   └─ getter.Get(key) 回源 → cloneBytes 深拷贝 → populateCache 写回
```

## 二、分层模块（自底向上）

### 第 1 层：LRU 淘汰 + TTL 过期（`lru/lru.go`）
- 数据结构：**双向链表 + map**。链表维护访问顺序（Front 最近 / Back 最久），map 做 O(1) 查找。
- 淘汰：内存超限时从队尾 `RemoveOldest` 循环淘汰。
- TTL：过期时间 `expiresAt` 存在每个 `entry` 里，`ttl>0` 才设置、零值永不过期。
  - **惰性过期**：`Get` 命中时判断，过期就删掉返回 miss。
  - **主动清理**：`CleanupExpired()` 从队尾向前扫，删全部过期条目（外部要先加锁）。

### 第 2 层：只读字节视图 ByteView（`byteview.go`）
- 私有字段 `b []byte`，对外**不给原切片、只给深拷贝**（`ByteSlice()` → `cloneBytes`），防止外部篡改缓存。
- 实现 `Len()` 让 LRU 能统计它占多少内存。

### 第 3 层：并发安全的缓存封装（`cache.go`）
- `cache` 结构 = 一把 `sync.Mutex` + 一个 `*lru.Cache`。`add`/`get` 就是 `mu.Lock()`/`Unlock()` 包着调 `lru.Add`/`lru.Get`。
- **懒初始化**：第一次 `add` 才 `lru.New(...)`，没写过数据的 Group 不分配 LRU。
- **后台清理**：`StartCleanup(interval)` 起 goroutine，`ticker := time.NewTicker(interval)`，`for range ticker.C` 周期持锁 `CleanupExpired`。

### 第 4 层：源数据回调 Getter（`geecache.go`）
- `type Getter interface { Get(key string) ([]byte, error) }`——**找不到缓存时去哪里加载原始数据**，由用户自己用回调实现。
- `GetterFunc` 是**函数适配器**：让普通函数实现接口，传匿名函数即可。

### 第 5 层：核心调度 Group（`geecache.go`）
- Group 是独立缓存命名空间，对外查询入口，串联：本地缓存查询 → 分布式节点路由 → 回源。
- **singleflight 防击穿**：同一个 key 大量并发 miss 时，只放行 1 个协程去回源，其余阻塞等待复用结果。
- **熔断器容错**：远程节点连续失败 N 次后熔断，跳过远程直接降级到本地回源。
- **SetTTL(ttl, cleanupInterval)**：设默认过期时间 + 可选启动后台清理。

### 第 6 层：节点通信（`http.go` + `peers_grpc.go`）
- **PeerPicker 接口**做节点管理：`PickPeer(key)` 用一致性哈希选节点。
- **PeerGetter 接口**做远程取数：`Get(in *pb.Request, out *pb.Response) error`，protobuf 序列化。
- 两种实现：
  - **HTTPPool**：HTTP 服务端（接收其他节点查询）+ 内置 `httpGetter` 客户端；用独立请求头做简单鉴权，可开 TLS/HTTPS 加密内网通信。
  - **GRPCPool**：gRPC 服务端 `GRPCServer` + 客户端 `grpcGetter`，共用同一套接口和 proto 定义。

### 第 7 层：一致性哈希环（`consistenthash/consistenthash.go`）
- `crc32` 做哈希，每个真实节点生成 `replicas`（50）个虚拟节点，标识为 `i + key`。
- 所有虚拟节点哈希值排序成环，`Get(key)` 二分查找第一个 `>= hash(key)` 的位置，取模回绕。

### 第 8 层：服务发现（`discovery.go` + `filediscovery.go`）
- `ServiceDiscovery` 接口抽象「获取/注册/监听节点列表」。
- `FileDiscovery` 基于共享 `peers.json`，后台每 3 秒读一次，节点列表变化时回调 → `Pool.Set(peers...)` 重建哈希环，**动态更新 peer 列表**。

### 第 9 层：指标埋点 + 健康检查 + 优雅关闭
- **指标**（`metrics.go`）：`sync/atomic` 计数命中/未命中/本地加载/远程加载/淘汰，`/metrics` 暴露命中率等。
- **健康检查**：`/health` 返回 200 "OK"。
- **优雅关闭**（`cmd/main.go`）：监听 SIGINT/SIGTERM → `http.Server.Shutdown` 等请求处理完再退（10s 超时）。

## 三、熔断器状态流转

```
Closed（闭合，正常放行）
   ↓ 连续失败达到阈值 N 次
Open（熔断打开，拒绝请求）
   ↓ 等待 timeout 时间到期
HalfOpen（半开，只放行探测请求）
   ├─ 探测请求成功 → 切回 Closed，重置失败计数
   └─ 探测请求失败 → 切回 Open，继续熔断
```

- 本项目：`NewCircuitBreaker(3, 10*time.Second)`，连续失败 3 次熔断、10 秒后尝试恢复。
- 接入点：`getFromPeer` —— 熔断打开时直接快速失败，上层 `load` 自动降级到 `getLocal` 回源。
