package geecache

import (
	"fmt"
	"sync"
	"time"
)

// =============================================================================
// CircuitBreaker — 熔断器，保护本节点不被故障远端拖垮
//
// 状态机：
//   Closed ──连续N次失败──► Open ──timeout到期──► HalfOpen
//     ▲                                              │
//     └────── 请求成功 ◄──────────────────────────────┘
//               请求失败 → 重新回到 Open
// =============================================================================

type State int

const (
	StateClosed   State = iota // 正常：允许请求
	StateOpen                  // 熔断：直接拒绝，快速失败
	StateHalfOpen              // 半开：放行一个探测请求
)

// cc
// ErrCircuitOpen 熔断打开时返回的错误，调用方据此触发降级
var ErrCircuitOpen = fmt.Errorf("circuit breaker is open")

type CircuitBreaker struct {
	mu            sync.Mutex
	state         State
	failureCount  int
	failureThresh int           // 连续失败 N 次 → 熔断
	timeout       time.Duration // 熔断多久后尝试恢复
	lastFailure   time.Time
}

// NewCircuitBreaker 创建熔断器，thresh 为失败阈值，timeout 为熔断恢复等待
func NewCircuitBreaker(thresh int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:         StateClosed,
		failureThresh: thresh,
		timeout:       timeout,
	}
}

// IsOpen 返回 true 表示熔断中，调用方应跳过远程请求直接降级
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return false
	case StateOpen:
		// 到了恢复时间 → 进入半开状态，放行一个探测请求
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = StateHalfOpen
			return false // 半开状态允许请求
		}
		return true // 熔断中，拒绝
	case StateHalfOpen:
		return false // 放行
	}
	return false
}

// RecordSuccess 请求成功时调用：半开→关闭，重置计数
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = StateClosed
	cb.failureCount = 0
}

// RecordFailure 请求失败时调用：递增计数，达到阈值进入熔断
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailure = time.Now()

	if cb.failureCount >= cb.failureThresh || cb.state == StateHalfOpen {
		// 连续失败达阈值 或 半开探测失败 → 进入熔断
		cb.state = StateOpen
	}
}
