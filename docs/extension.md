# tutucache 扩展指南

> 本文按「当前已实现」与「下一步可扩展」两部分梳理 tutucache 的能力边界。
> 已实现的部分给出真实行为和关键文件，待实现的部分给出方案与取舍，避免把设计稿误当成现状。

状态标记：

- **已实现**：代码、示例二进制和测试都已覆盖。
- **待扩展**：仍是设计方向，仓库中尚未实现。

---

## 目录

1. [已实现能力](#一已实现能力)
   - [1.1 gRPC 通信](#11-grpc-通信)
   - [1.2 TTL 过期机制](#12-ttl-过期机制)
   - [1.3 指标与监控](#13-指标与监控)
   - [1.4 优雅关闭与健康检查](#14-优雅关闭与健康检查)
2. [生产加固现状与剩余项](#二生产加固现状与剩余项)
   - [2.1 服务发现与动态节点](#21-服务发现与动态节点)
   - [2.2 Circuit Breaker 熔断](#22-circuit-breaker-熔断)
   - [2.3 TLS / 认证](#23-tls--认证)
   - [2.4 数据持久化](#24-数据持久化)
3. [进阶特性（待实现）](#三进阶特性待实现)
   - [3.1 缓存写入策略](#31-缓存写入策略)
   - [3.2 批量操作](#32-批量操作)
   - [3.3 事件通知与失效](#33-事件通知与失效)
   - [3.4 数据复制](#34-数据复制)
4. [实施路线图](#四实施路线图)

---

## 一、已实现能力

### 1.1 gRPC 通信

**状态：已实现。** `peers_grpc.go` 提供 `GRPCServer`、`grpcGetter` 与 `GRPCPool`，与 HTTP 传输共用
`PeerPicker` / `PeerGetter` 接口和 protobuf 序列化。示例二进制通过 `-transport=grpc` 选择 gRPC 路径。

**能力清单：**

- `Set` / `PickPeer`：把 peer 归一化为裸 `host:port`，一致性哈希选节点；拓扑变化按差集增删，
  幸存连接继续复用，下线 peer 的连接在锁外关闭。
- `StartDiscovery(d)`：注册裸 `host:port` 并监听节点变更，与 `FileDiscovery` 直接兼容。
- `SetSharedToken`：客户端/服务端各一个 unary interceptor，每次 RPC 读取当前 token；
  metadata key 使用小写 `x-geecache-token`。认证失败返回 `Unauthenticated`，
  `peererror.go` 将其归类为配置错误，不会静默降级到本地。
- `EnableTLS`：与 HTTP 共用 `buildPeerTLSConfigs`（TLS 1.2、可选 mTLS）。
  gRPC server 的凭据在 `grpc.NewServer` 时固定，因此只能在 `Serve`/`ServeListener` 之前设置；
  客户端 TLS 变更会重建已有连接。
- `Serve` / `ServeListener`：`ServeListener` 接受预绑定 listener，便于测试和自定义监听；
  server 惰性创建，绑定失败不会把 pool 标记为已启动。
- `Shutdown(ctx)` / `Stop()`：先 `GracefulStop`（不持锁），超时后强制 `Stop`；
  等在途 handler 结束后再关闭客户端连接，避免切断仍在转发的请求。

**地址约定：** gRPC target 与哈希环统一使用裸 `host:port`；带 scheme 的 URL 会去掉
scheme/path。HTTP 与 gRPC 节点不能混用同一个 `peers.json`：HTTP 注册 URL，gRPC 注册裸地址。

**剩余扩展点：** 可抽象出 `Transport` 接口（`Serve` + `PeerPicker`/`PeerGetter`），
由工厂根据配置构造，替代 `cmd` 中的 if/else 分支：

```go
type Transport interface {
    Serve() error
    PeerGetter
    PeerPicker
}
```

### 1.2 TTL 过期机制

**状态：已实现。** 本地缓存同时支持容量淘汰和时间过期。

- `lru.entry` 保存 `expiresAt`，`ttl <= 0` 表示永不过期。
- **惰性过期**：`lru.Get` 命中时检查过期，过期即删除并返回 miss；保证读到的值不会过期。
- **主动清理**：`Group.SetTTL(ttl, cleanupInterval)` 在 `cleanupInterval > 0` 时启动后台 ticker，
  分批扫描并删除过期条目；重复调用不会重复启动 goroutine。
- **摊销回收**：`cleanupInterval == 0` 时没有后台循环，但每次写入会从冷端扫描少量条目，
  避免长期不访问的过期 key 永久占住 LRU 槽位。
- `Purge()` 清空本机该 Group 的本地缓存；`Group.Close()` 停止后台清理循环，可重复调用。
- 过期/淘汰回调在缓存锁外执行，避免用户回调重入缓存导致死锁。

**语义边界：**

- `SetTTL` 是**前瞻生效**的：过期时间在写入时固化，修改 TTL 不会追溯已有条目。
  切换策略时建议先 `SetTTL` 再 `Purge`；两者不是原子的，`Purge` 不等待在途加载。
- 目前只有 Group 级默认 TTL，没有逐 key 的 `SetWithTTL`。
- 后台清理间隔在首次启动后固定，不支持动态调整。

**可继续扩展：** 逐 key TTL、TTL 随机抖动（避免同一时刻大面积过期）、
把清理间隔改为可热更新配置。

### 1.3 指标与监控

**状态：已实现。** `metrics.go` 按 Group 隔离指标，用 Prometheus 文本格式暴露。

- 每个 `Group` 持有独立的 `Metrics`，统计 hits / misses / local loads / peer loads / evictions。
- 写入与 `Snapshot` 共用一把 `sync.RWMutex`，一次拷贝全部字段，返回同一时刻的一致视图。
- `/metrics` 输出带 `group="..."` 标签的原始 `_total` 计数器；不导出 lifetime 命中率 gauge，
  近期命中率由 Prometheus 对计数器求 `rate()`。
- `geecache_cache_bytes` 是 gauge：抓取时在 `cache.mu` 保护下读取 LRU 的实时占用
  （`lru.Bytes()` → `cache.Bytes()` → `Group.CacheBytes()`），不额外维护容易漂移的镜像。
- HTTP 模式由节点 handler 直接暴露 `/metrics`；gRPC 模式通过 `-admin-addr` 启动独立管理端口，
  同时提供 `/metrics` 与 `/health`。管理端口是明文 HTTP，不参与节点间 mTLS。

**可继续扩展：** 延迟直方图、连接数/重连次数、熔断器状态、命中率按时间窗口聚合、
接入 OpenTelemetry 或 `prometheus/client_golang`。

### 1.4 优雅关闭与健康检查

**状态：已实现。**

- `cmd/main.go` 监听 `SIGINT` / `SIGTERM`，按固定顺序关闭：
  1. `discovery.Close()` 停止轮询与心跳；
  2. `discovery.Unregister(registeredAddr)` 注销本节点，避免心跳把节点重新写回；
  3. HTTP 入口（API、admin、cache）共享 10 秒超时执行 `Shutdown`；
  4. gRPC 调用 `GRPCPool.Shutdown(ctx)`：`GracefulStop` 等待在途 RPC，超时后强制 `Stop`；
  5. `Group.Close()` 停止 TTL 清理循环。
- 所有 HTTP server 统一配置 `ReadHeaderTimeout` / `ReadTimeout` / `WriteTimeout` / `IdleTimeout`，
  避免慢连接无限占用资源。
- `/health` 返回 `200 OK`；HTTP 模式由节点 handler 提供，gRPC 模式通过 `-admin-addr` 提供。

**可继续扩展：** readiness（检查数据源、服务发现连接等依赖）、
把关闭超时和后台任务停止信号统一做成可配置的 context 树。

---

## 二、生产加固现状与剩余项

### 2.1 服务发现与动态节点

**状态：已实现（文件发现版）。**

```go
type ServiceDiscovery interface {
    GetPeers() ([]string, error)
    Register(addr string) error
    Unregister(addr string) error
    Watch(onChange func([]string))
}
```

- `FileDiscovery` 基于共享 `peers.json`：
  - `Register` 使用跨进程文件锁 + 原子替换，避免并发注册丢更新；
  - 后台每 3 秒轮询并在节点列表变化时回调，同时周期性重注册（心跳）；
  - `Unregister` 删除本节点，`Close` 停止轮询/心跳，重复调用安全。
- `HTTPPool` / `GRPCPool` 的 `StartDiscovery` 注册本节点，并在回调中调用 `Set`；
  `Set` 按差集增量更新哈希环与连接，不重建整环。

**剩余项：** `FileDiscovery` 没有 lease/健康检查，节点崩溃后要等文件被清理或心跳过期；
3 秒轮询不是实时感知。生产可替换为 etcd / consul / zookeeper 或 DNS SRV：

| 方案 | 适用场景 |
|------|----------|
| etcd / consul | 生产环境，需要强一致性与 lease |
| DNS SRV | Kubernetes Headless Service |
| 配置文件 + 热加载 | 小规模部署 |
| Redis Pub/Sub | 已有 Redis 设施的团队 |

### 2.2 Circuit Breaker 熔断

**状态：已实现，粒度为 (Group, peer)。**

- `Group.getFromPeer` 通过 `breakerFor(peer)` 取熔断器；内置 `httpGetter` / `grpcGetter`
  实现 `PeerBreakerProvider`，每个 getter 持有按 Group 名索引的熔断器。
- 默认参数：30 秒滑动窗口内失败 3 次 → 熔断 10 秒；冷却到期后只放行一个探测请求（HalfOpen）。
- `Ticket.generation` 丢弃跨状态迁移的迟到结果；`probeInFlight` 保证半开只有一个探测。
- 失败按时间窗口统计，窗口外的旧失败会被丢弃，零星慢失败不会长期累积。
- 配置错误和正常 key 不存在走 `RecordNeutral`，只释放探测名额，不计失败。
- peer 下线或客户端重建时 getter 被丢弃，熔断状态随之回收。

**剩余项：** 阈值、窗口、冷却时间目前是固定默认值，可改为按 Group/peer 配置；
可增加熔断状态和拒绝次数指标，便于告警。

### 2.3 TLS / 认证

**状态：已实现，HTTP 与 gRPC 共用同一套语义。**

- **共享 token**：
  - HTTP 使用 `X-Geecache-Token` 请求头；gRPC 使用同名小写 metadata key。
  - 比较走 SHA-256 + `subtle.ConstantTimeCompare`，避免时序侧信道。
  - gRPC 每次 RPC 读取 token，轮换不需要重连；HTTP 在配置变更时重建 getter。
- **TLS / mTLS**：
  - `buildPeerTLSConfigs` 同时构造服务端/客户端配置，最低 TLS 1.2。
  - `GEECACHE_CERT` / `GEECACHE_KEY` 必成对出现；`GEECACHE_CA` 非空时启用 mTLS，
    服务端使用 `ClientCAs` + `RequireAndVerifyClientCert`，客户端使用 `RootCAs`。
  - 同一张节点证书同时承担服务端/客户端角色，因此需要相应 SAN 与 `clientAuth` EKU。
  - HTTP 模式注册 `https://` 地址；gRPC 模式仍注册裸 `host:port`，TLS 由 transport credentials 启用。
  - gRPC server 凭据启动后不可替换；客户端 TLS 可热切换但会重建连接并重置该 peer 的熔断状态。
- 管理端口和前端 API 端口是明文 HTTP；TLS 只保护节点间通信。

**剩余项：** 证书热加载/轮换、把 token 接入 secret manager、按 group/方法做细粒度授权、
管理端口的独立认证。

### 2.4 数据持久化

**状态：待实现。** 当前是纯内存缓存，进程重启后数据全部丢失，重启瞬间会有较多回源。

可选方案：

```go
type Persister interface {
    Save(key string, value []byte, expiresAt time.Time) error
    Load(key string) ([]byte, time.Time, error)
    Delete(key string) error
    LoadAll() (map[string]CacheEntry, error)
}
```

- BoltDB / Badger 适合嵌入式单机恢复；Redis 适合已有基础设施的场景。
- 写入会增加延迟，可异步批量写、只采样热点数据，并给持久化文件设置容量上限。
- 恢复时应校验 `expiresAt`，过期数据不能重新进入缓存。

---

## 三、进阶特性（待实现）

### 3.1 缓存写入策略

**现状：** 只有 `Get`（读时加载），没有对外的 Set/Delete/Update。

**扩展方向：**

```go
func (g *Group) Set(key string, value []byte, ttl time.Duration) error
func (g *Group) Delete(key string) error
```

需要同时定义写入模式：WriteBack（先写缓存、异步刷 DB）、WriteThrough（缓存与 DB 同步写）、
WriteAround（只写 DB、读时进缓存）。多节点场景还要考虑失效广播与版本号，
否则不同节点会读到旧值。

### 3.2 批量操作

**现状：** 每次只能 `Get` 一个 key。

**扩展方向：** 提供 `MGet(keys []string)`，先按 key 归属分组，每个远程节点只发一次批量请求；
在 protobuf 中增加批量 message：

```protobuf
message BatchRequest {
    string group = 1;
    repeated string keys = 2;
}
message BatchResponse {
    map<string, bytes> values = 1;
}
```

需要注意单个 key 失败不应让整批失败，以及批量请求的超时和响应大小上限。

### 3.3 事件通知与失效

**现状：** 缓存与 DB 之间没有联动，DB 更新后缓存中的旧值会保留到 TTL 或淘汰。

**扩展方向：** 引入事件总线（Redis Pub/Sub、Kafka、NATS 或 gRPC stream），
在 Set/Delete/Expire 时广播失效事件；消费端按 group/key 删除本地缓存。
需要处理事件乱序、重复投递和节点离线期间的补偿。

### 3.4 数据复制

**现状：** 每个 key 只存在哈希环选中的节点上；节点宕机后对应缓存全部丢失，需要回源。

**扩展方向：** 一致性哈希 `GetN(key, n)` 返回顺时针 N 个节点，主节点同步/异步复制到备份节点。
需要定义副本一致性、故障切换和回源策略；对缓存场景通常优先可用性而不是强一致。

---

## 四、实施路线图

```
已完成                                 下一步可选
──────────────────────────────────     ─────────────────────────────
TTL（惰性 + 后台 + 摊销回收）           证书/token 热加载与轮换
按 Group 指标 + /metrics               批量操作 (MGet/MSet)
健康检查 + 独立 admin 端口              逐 key TTL / TTL 抖动
HTTP/gRPC 优雅关闭                      写策略与事件失效
文件服务发现 + 增量拓扑更新              数据复制 (N=3)
(Group, peer) 熔断 + 滑动窗口           持久化与预热
HTTP/gRPC token + TLS/mTLS              Admin UI / OpenTelemetry
gRPC 服务发现与可运行入口
```

---

## 总结

tutucache 已经具备一个可运行的分布式缓存核心：LRU + TTL、一致性哈希、singleflight、
per-(Group, peer) 熔断、动态服务发现、按 Group 指标、健康检查、HTTP/gRPC 双传输与优雅关闭。

继续扩展时优先解决真实瓶颈，而不是堆功能：

- **个人项目/学习**：已经足够，可补逐 key TTL 和更细指标。
- **团队内部工具**：优先证书/token 轮换、readiness、批量操作。
- **公司核心服务**：再考虑持久化、复制、事件失效和管理端认证。

记住一个原则：**先明确一致性与可用性边界，再决定加什么**。缓存系统的大部分复杂度，
都来自多节点之间的状态同步与故障处理。
