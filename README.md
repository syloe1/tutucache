# tutucache

> 一个用 Go 语言实现的**分布式缓存系统**，灵感来源于 极客兔兔 的 [geecache](https://geektutu.com/post/geecache.html)。

## 概述

tutucache 是一个分布式内存缓存库，适用于需要**高并发读取**、**低延迟响应**的场景。它支持多节点部署，通过一致性哈希在节点间分配 key，节点间支持 **HTTP 与 gRPC 两种传输**（统一 protobuf 序列化），并内置 singleflight 防击穿、熔断器降级、服务发现、指标埋点、健康检查与优雅关闭。

## 特性

- **LRU 淘汰 + TTL 过期**：本地缓存使用 LRU 算法，支持内存上限、淘汰回调，以及按时间过期（惰性过期 + 后台主动清理）
- **一致性哈希**：虚拟节点 + 一致性哈希环，节点增减时最小化缓存迁移
- **Singleflight 合并**：相同 key 的并发请求合并为一次调用，防止缓存击穿
- **熔断降级**：三态熔断器（Closed/Open/HalfOpen），远程节点故障时快速失败并降级到本地回源
- **服务发现**：`ServiceDiscovery` 接口 + 文件发现实现，动态更新节点列表、重建哈希环
- **双传输 + Protobuf**：HTTP 与 gRPC 两种通道共用 `PeerPicker`/`PeerGetter` 接口，序列化统一 protobuf
- **只读字节视图**：`ByteView` 确保缓存值不会被外部意外修改
- **指标埋点**：命中率、本地/远程加载次数等（Prometheus 格式 `/metrics`）
- **健康检查**：`/health` 存活探针
- **优雅关闭**：监听退出信号，`http.Server.Shutdown` 等待请求处理完成
- **接口解耦**：通过 `PeerPicker` / `PeerGetter` / `ServiceDiscovery` 接口隔离节点发现、网络通信与拓扑来源

## 架构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  API Server │     │ Cache Node  │     │ Cache Node  │
│  (:9999)    │     │  (:8001)    │     │  (:8002)    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │  Get("Tom")       │                   │
       ├──────────────────►│                   │
       │                   │                   │
       │  ① 查本地 LRU      │                   │
       │  ② miss → 一致性哈希│                   │
       │  ③ PickPeer → :8002│                  │
       │                   │──HTTP/gRPC───────►│
       │                   │◄───pb.Response────│
       │◄──────value────────│                   │
```

## 项目结构

```
tutucache/
├── lru/                    # LRU 缓存淘汰算法（含 TTL 过期）
│   ├── lru.go
│   └── lru_test.go
├── consistenthash/         # 一致性哈希环
│   ├── consistenthash.go
│   └── consistenthash_test.go
├── singleflight/           # 请求合并，防缓存击穿
│   └── singlefight.go
├── geecachepb/             # Protobuf 定义与生成代码
│   ├── geecachepb.proto
│   ├── geecachepb.pb.go
│   └── geecachepb_grpc.pb.go
├── byteview.go             # 只读字节视图
├── cache.go                # 并发安全 LRU 缓存封装（含后台清理）
├── peers.go                # PeerPicker / PeerGetter 接口定义
├── http.go                 # HTTP 节点池（服务端 + 客户端，TLS/token）
├── peers_grpc.go           # gRPC 节点池（服务端 + 客户端）
├── discovery.go            # 服务发现接口定义
├── filediscovery.go        # 基于 peers.json 的文件服务发现
├── circuitbreaker.go       # 三态熔断器
├── metrics.go              # 指标埋点（/metrics）
├── geecache.go             # 核心：Group 缓存命名空间
├── geecache_test.go        # 核心测试
├── circuitbreaker_test.go  # 熔断器测试
├── peers_grpc_test.go      # gRPC 链路测试
├── docs/                   # 学习/理解/扩展/面试文档
├── cmd/
│   └── main.go             # 可运行示例
├── run.sh                  # bash 一键启动脚本（Linux / macOS）
├── run.py                  # Python 一键启动脚本（跨平台，主要面向 Windows）
├── go.mod
└── go.sum
```

## 快速开始

### 安装

```bash
git clone https://github.com/syloe1/tutucache.git
cd tutucache
go mod tidy
```

### 运行示例

**方式一：使用脚本一键启动（推荐）**

Linux / macOS：

```bash
chmod +x run.sh
./run.sh
```

Windows（或任何有 Python3 的环境）：

```bash
python run.py
# 按 Ctrl+C 退出
```

脚本会自动编译、启动 3 个缓存节点、等待健康检查通过，并发起测试请求。

**方式二：手动启动**

启动三个缓存节点 + 一个 API 服务：

```bash
# 终端1：启动节点 :8001
go run cmd/main.go -port=8001

# 终端2：启动节点 :8002
go run cmd/main.go -port=8002

# 终端3：启动节点 :8003 + API 服务 :9999
go run cmd/main.go -port=8003 -api=true
```

访问 API：

```bash
curl "http://localhost:9999/api?key=Tom"
# 输出: 630

curl "http://localhost:9999/api?key=Jack"
# 输出: 589
```

### 在代码中使用

```go
import "geecache"

// 1. 创建缓存 Group
group := geecache.NewGroup("users", 1<<20, geecache.GetterFunc(
    func(key string) ([]byte, error) {
        // 从数据库或其他数据源加载数据
        return db.Query(key), nil
    },
))

// 2. 设置节点池
pool := geecache.NewHTTPPool("http://localhost:8001")
pool.Set(
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
)
group.RegisterPeers(pool)

// 3. （可选）设置 TTL 过期 + 后台清理
group.SetTTL(5*time.Minute, 30*time.Second)

// 4. 作为 HTTP 服务启动
http.ListenAndServe(":8001", pool)

// 5. 获取缓存
value, err := group.Get("user:123")
```

## 核心 API

### Group

| 方法 | 说明 |
|------|------|
| `NewGroup(name, cacheBytes, getter)` | 创建缓存组 |
| `GetGroup(name)` | 获取已创建的缓存组 |
| `Get(key)` | 获取缓存值（自动处理 miss） |
| `RegisterPeers(peers)` | 注册远程节点 |
| `SetTTL(ttl, cleanupInterval)` | 设置默认过期时间，可选启动后台清理 |

### HTTPPool

| 方法 | 说明 |
|------|------|
| `NewHTTPPool(self)` | 创建 HTTP 节点池 |
| `Set(peers...)` | 设置集群节点地址 |
| `ServeHTTP(w, r)` | 实现 `http.Handler`，处理节点间请求 |
| `StartDiscovery(d)` | 注册并监听节点变更 |
| `SetSharedToken(token)` | 开启共享 token 认证 |
| `EnableTLS(cert, key, ca)` | 开启 TLS/mTLS 加密 |

## 许可证

MIT License
