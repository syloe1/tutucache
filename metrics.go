package geecache

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// =============================================================================
// 全局指标
// =============================================================================

type Metrics struct {
	Hits       int64 // 命中次数
	Misses     int64 // 未命中次数
	LocalLoads int64 // 从本地数据源加载次数
	PeerLoads  int64 // 从远程节点加载次数
	Evictions  int64 // 淘汰条目数
	TotalBytes int64 // 当前占用内存（近似值）
}

var globalMetrics = &Metrics{}

// =============================================================================
// 写入方法 — 在关键路径上由 Group / cache 调用
// =============================================================================

func (m *Metrics) RecordHit()       { atomic.AddInt64(&m.Hits, 1) }
func (m *Metrics) RecordMiss()      { atomic.AddInt64(&m.Misses, 1) }
func (m *Metrics) RecordLocalLoad() { atomic.AddInt64(&m.LocalLoads, 1) }
func (m *Metrics) RecordPeerLoad()  { atomic.AddInt64(&m.PeerLoads, 1) }
func (m *Metrics) RecordEviction()  { atomic.AddInt64(&m.Evictions, 1) }
func (m *Metrics) SetTotalBytes(n int64) {
	atomic.StoreInt64(&m.TotalBytes, n)
}

// =============================================================================
// 读取方法 — 供外部查询
// =============================================================================
// 命中率
func (m *Metrics) HitRate() float64 {
	hits := atomic.LoadInt64(&m.Hits)
	misses := atomic.LoadInt64(&m.Misses)
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// 指标快照，返回一份值拷贝
func (m *Metrics) Snapshot() Metrics {
	return Metrics{
		Hits:       atomic.LoadInt64(&m.Hits),
		Misses:     atomic.LoadInt64(&m.Misses),
		LocalLoads: atomic.LoadInt64(&m.LocalLoads),
		PeerLoads:  atomic.LoadInt64(&m.PeerLoads),
		Evictions:  atomic.LoadInt64(&m.Evictions),
		TotalBytes: atomic.LoadInt64(&m.TotalBytes),
	}
}

// =============================================================================
// HTTP 端点 — 注册到 HTTPPool 的路由中
// =============================================================================

// MetricsHandler 返回一个 http.Handler，对外暴露 Prometheus 风格指标
func MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := globalMetrics.Snapshot()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "# HELP geecache_hits_total Total number of cache hits\n")
		fmt.Fprintf(w, "# TYPE geecache_hits_total counter\n")
		fmt.Fprintf(w, "geecache_hits_total %d\n", s.Hits)
		fmt.Fprintf(w, "# HELP geecache_misses_total Total number of cache misses\n")
		fmt.Fprintf(w, "# TYPE geecache_misses_total counter\n")
		fmt.Fprintf(w, "geecache_misses_total %d\n", s.Misses)
		fmt.Fprintf(w, "# HELP geecache_local_loads_total Total number of local loads\n")
		fmt.Fprintf(w, "# TYPE geecache_local_loads_total counter\n")
		fmt.Fprintf(w, "geecache_local_loads_total %d\n", s.LocalLoads)
		fmt.Fprintf(w, "# HELP geecache_peer_loads_total Total number of peer loads\n")
		fmt.Fprintf(w, "# TYPE geecache_peer_loads_total counter\n")
		fmt.Fprintf(w, "geecache_peer_loads_total %d\n", s.PeerLoads)
		fmt.Fprintf(w, "# HELP geecache_evictions_total Total number of evictions\n")
		fmt.Fprintf(w, "# TYPE geecache_evictions_total counter\n")
		fmt.Fprintf(w, "geecache_evictions_total %d\n", s.Evictions)
		fmt.Fprintf(w, "# HELP geecache_hit_rate Cache hit rate\n")
		fmt.Fprintf(w, "# TYPE geecache_hit_rate gauge\n")
		fmt.Fprintf(w, "geecache_hit_rate %.4f\n", s.HitRate())
		fmt.Fprintf(w, "# HELP geecache_cache_bytes Current cache size in bytes\n")
		fmt.Fprintf(w, "# TYPE geecache_cache_bytes gauge\n")
		fmt.Fprintf(w, "geecache_cache_bytes %d\n", s.TotalBytes)
	})
}
