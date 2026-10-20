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
//     └────── 探测成功 ◄─────────────────────────────┤
//               探测失败 → 重新回到 Open
//
// 并发正确性依赖两个独立机制：
//   generation    — 每次状态迁移自增；ticket 记录获准时的代数，
//                   迟到的旧结果因代数不匹配而整体作废。
//   probeInFlight — HalfOpen 的探测名额；Open→HalfOpen 与名额预约在同一把锁内完成，
//                   保证半开期间只放行一个探测请求。
// =============================================================================

type State int

const (
	StateClosed   State = iota // 正常：允许请求
	StateOpen                  // 熔断：直接拒绝，快速失败
	StateHalfOpen              // 半开：放行一个探测请求
)

// ErrCircuitOpen 熔断打开时返回的错误，调用方据此触发降级
var ErrCircuitOpen = fmt.Errorf("circuit breaker is open")

// Ticket 是准入凭证，零值恒为无效
type Ticket struct {
	generation uint64 // 获准时的状态代数
}

type CircuitBreaker struct {
	mu            sync.Mutex
	state         State
	failureCount  int
	failureThresh int           // 连续失败 N 次 → 熔断
	timeout       time.Duration // 熔断多久后尝试恢复
	lastFailure   time.Time     // 仅在进入 Open 时设置，冷却从这里开始计算
	generation    uint64        // 当前状态代数，从 1 开始，每次状态迁移自增
	probeInFlight bool          // HalfOpen 探测名额是否已被占用
}

// NewCircuitBreaker 创建熔断器，thresh 为失败阈值，timeout 为熔断恢复等待
func NewCircuitBreaker(thresh int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:         StateClosed,
		generation:    1, // 从 1 开始，保证零值 Ticket 永远失效
		failureThresh: thresh,
		timeout:       timeout,
	}
}

// Allow 申请准入。ok 为 false 表示熔断中，调用方应跳过远程请求直接降级；
// ok 为 true 时，调用方必须用返回的 ticket 恰好回填一次结果（成功/失败/中立）。
// 漏掉回填会让 HalfOpen 的探测名额永久占用。
func (cb *CircuitBreaker) Allow() (Ticket, bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return Ticket{generation: cb.generation}, true

	case StateOpen:
		if time.Since(cb.lastFailure) < cb.timeout {
			return Ticket{}, false // 冷却未到，拒绝
		}
		// 冷却到期：进入半开；探测名额在下面的公共逻辑中预约
		cb.state = StateHalfOpen
		cb.generation++
		cb.probeInFlight = false

	case StateHalfOpen:
		// 探测名额的判断在下面统一处理
	}

	// 半开：只有探测名额空闲时才放行，并原子地占住它。
	// 当前请求就是探测请求，其他请求会看到 probeInFlight=true 而被拒绝。
	if cb.probeInFlight {
		return Ticket{}, false
	}
	cb.probeInFlight = true
	return Ticket{generation: cb.generation}, true
}

// RecordSuccess 请求成功时调用：Closed 下重置计数，HalfOpen 探测成功则关闭。
// 失效 ticket（状态已迁移）不改动任何状态。
func (cb *CircuitBreaker) RecordSuccess(t Ticket) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if t.generation != cb.generation {
		return // 旧代数的迟到结果，熔断器已经进入新状态
	}

	switch cb.state {
	case StateClosed:
		cb.failureCount = 0

	case StateHalfOpen:
		// 当前代数 + HalfOpen 只可能是预约的探测票据
		if !cb.probeInFlight {
			return // 没有预约的探测，不接受结果
		}
		cb.probeInFlight = false
		cb.state = StateClosed
		cb.failureCount = 0
		cb.generation++
	}
}

// RecordNeutral 释放 HalfOpen 的探测名额，但不改变失败计数。
// 用于「对端有明确响应、但属于配置错误」的场景：既不是远端故障，也不是成功。
// 探测名额必须释放，否则 HalfOpen 会永久卡住，后续请求全部静默降级。
func (cb *CircuitBreaker) RecordNeutral(t Ticket) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if t.generation != cb.generation || cb.state != StateHalfOpen || !cb.probeInFlight {
		return
	}
	cb.probeInFlight = false
}

// RecordFailure 请求失败时调用：递增连续失败计数，达到阈值进入熔断；
// HalfOpen 探测失败则重新熔断，并从现在开始计算冷却。
// 失效 ticket 既不计数，也不修改 lastFailure。
func (cb *CircuitBreaker) RecordFailure(t Ticket) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if t.generation != cb.generation {
		return // 旧代数的迟到结果，不能影响当前状态
	}

	switch cb.state {
	case StateClosed:
		cb.failureCount++
		if cb.failureCount >= cb.failureThresh {
			cb.state = StateOpen
			cb.lastFailure = time.Now()
			cb.generation++
		}

	case StateHalfOpen:
		// 当前代数 + HalfOpen 只可能是预约的探测票据
		if !cb.probeInFlight {
			return // 没有预约的探测，不接受结果
		}
		cb.probeInFlight = false
		cb.state = StateOpen
		cb.lastFailure = time.Now() // 重新计算冷却起点
		cb.generation++
	}
}
