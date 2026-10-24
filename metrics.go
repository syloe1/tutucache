package geecache

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// =============================================================================
// 按 Group 隔离的指标
//
// 每个 Group 持有独立的 Metrics 实例，避免多个命名空间互相污染。
// 写入与快照共用同一把锁，保证 Snapshot 是某一时刻的一致视图。
// =============================================================================

type Metrics struct {
	mu         sync.RWMutex
	hits       int64 // 命中次数
	misses     int64 // 未命中次数
	localLoads int64 // 从本地数据源加载次数
	peerLoads  int64 // 从远程节点加载次数
	evictions  int64 // 淘汰条目数
}

// MetricsSnapshot 是 Metrics 在某一时刻的值拷贝。
// 单独定义而不复用 Metrics，避免拷贝内含的 RWMutex（go vet copylocks）。
type MetricsSnapshot struct {
	Hits       int64
	Misses     int64
	LocalLoads int64
	PeerLoads  int64
	Evictions  int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

// =============================================================================
// 写入方法 — 在关键路径上由 Group / cache 调用
// =============================================================================

func (m *Metrics) RecordHit()       { m.add(&m.hits) }
func (m *Metrics) RecordMiss()      { m.add(&m.misses) }
func (m *Metrics) RecordLocalLoad() { m.add(&m.localLoads) }
func (m *Metrics) RecordPeerLoad()  { m.add(&m.peerLoads) }
func (m *Metrics) RecordEviction()  { m.add(&m.evictions) }

func (m *Metrics) add(p *int64) {
	m.mu.Lock()
	*p++
	m.mu.Unlock()
}

// =============================================================================
// 读取方法 — 供外部查询
// =============================================================================

// HitRate 返回进程生命周期内的命中率（lifetime ratio），不是近期窗口速率。
// /metrics 不再导出该值；需要近期趋势请在 Prometheus 侧对原始计数器求 rate()。
func (m *Metrics) HitRate() float64 {
	s := m.Snapshot() // hits/misses 必须来自同一份快照
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Snapshot 在锁内一次性拷贝全部字段，返回某一时刻的一致视图。
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return MetricsSnapshot{
		Hits:       m.hits,
		Misses:     m.misses,
		LocalLoads: m.localLoads,
		PeerLoads:  m.peerLoads,
		Evictions:  m.evictions,
	}
}

// =============================================================================
// HTTP 端点 — 注册到 HTTPPool 的路由中
// =============================================================================

type groupMetrics struct {
	name       string
	snap       MetricsSnapshot
	cacheBytes int64 // 抓取时从 LRU 实时读取，不是 Metrics 里的计数器
}

// HealthHandler 返回健康检查 handler，供 HTTPPool 与独立 admin 端口共用。
func HealthHandler() http.Handler {
	return http.HandlerFunc(writeHealth)
}

// writeHealth 是 /health 的唯一实现：200 + "OK"。
func writeHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// MetricsHandler 返回一个 http.Handler，按 group 标签暴露 Prometheus 风格指标。
func MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 只在全局锁内拷贝 Group 指针，格式化输出时不持有全局锁
		mu.RLock()
		gs := make([]*Group, 0, len(groups))
		for _, g := range groups {
			gs = append(gs, g)
		}
		mu.RUnlock()

		// map 遍历顺序随机，排序保证输出稳定
		sort.Slice(gs, func(i, j int) bool { return gs[i].name < gs[j].name })

		rows := make([]groupMetrics, 0, len(gs))
		for _, g := range gs {
			rows = append(rows, groupMetrics{
				name:       g.name,
				snap:       g.metrics.Snapshot(),
				cacheBytes: g.mainCache.Bytes(),
			})
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		// 内存占用是 gauge：每次抓取都从 LRU 的实时状态派生。
		writeMetric(w, "geecache_cache_bytes", "Current number of bytes used by cached keys and values", "gauge", rows, func(r groupMetrics) int64 { return r.cacheBytes })
		writeMetric(w, "geecache_hits_total", "Total number of cache hits", "counter", rows, func(r groupMetrics) int64 { return r.snap.Hits })
		writeMetric(w, "geecache_misses_total", "Total number of cache misses", "counter", rows, func(r groupMetrics) int64 { return r.snap.Misses })
		writeMetric(w, "geecache_local_loads_total", "Total number of local loads", "counter", rows, func(r groupMetrics) int64 { return r.snap.LocalLoads })
		writeMetric(w, "geecache_peer_loads_total", "Total number of peer loads", "counter", rows, func(r groupMetrics) int64 { return r.snap.PeerLoads })
		writeMetric(w, "geecache_evictions_total", "Total number of evictions", "counter", rows, func(r groupMetrics) int64 { return r.snap.Evictions })
	})
}

// writeMetric 输出一个指标族：HELP/TYPE 各一次，然后每个 group 一条序列。
func writeMetric(w http.ResponseWriter, name, help, typ string, rows []groupMetrics, value func(groupMetrics) int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
	for _, r := range rows {
		fmt.Fprintf(w, "%s{group=%q} %d\n", name, r.name, value(r))
	}
}

//cc
