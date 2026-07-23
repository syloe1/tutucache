package geecache

import (
	"testing"
	"time"
)

func TestCircuitBreaker_ClosedToOpen(t *testing.T) {
	// 阈值 2：连续失败 2 次就熔断
	cb := NewCircuitBreaker(2, 5*time.Second)

	// 初始 Closed，不应该阻断
	if cb.IsOpen() {
		t.Fatal("expected closed initially")
	}

	// 第 1 次失败
	cb.RecordFailure()
	if cb.IsOpen() {
		t.Fatal("expected still closed after 1 failure")
	}

	// 第 2 次失败 → 达到阈值，进入 Open
	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected open after 2 failures")
	}
}

func TestCircuitBreaker_OpenToHalfOpen(t *testing.T) {
	// 阈值 1 + 超时 50ms
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	// 触发熔断
	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected open after failure")
	}

	// 立即查 → 仍然 Open
	if !cb.IsOpen() {
		t.Fatal("expected still open immediately")
	}

	// 等超时 → 进入 HalfOpen
	time.Sleep(60 * time.Millisecond)
	if cb.IsOpen() {
		t.Fatal("expected half-open after timeout, got still open")
	}
}

func TestCircuitBreaker_HalfOpenSuccess(t *testing.T) {
	// 阈值 2：半开成功后计数器重置，再来一次失败不会熔断
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	// 连败两次 → 触发熔断
	cb.RecordFailure()
	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected open after 2 failures")
	}

	time.Sleep(60 * time.Millisecond)

	// HalfOpen 状态下来一个成功 → 回到 Closed
	cb.RecordSuccess()
	if cb.IsOpen() {
		t.Fatal("expected closed after success in half-open")
	}

	// 再失败一次不应该立即熔断（计数器从 0 开始，需要 2 次才熔断）
	cb.RecordFailure()
	if cb.IsOpen() {
		t.Fatal("expected still closed, counter should be reset to 0 then 1 < thresh 2")
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	// Closed → 失败 → Open
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)

	// HalfOpen → 失败 → 回到 Open
	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected back to open after half-open failure")
	}
}

// =============================================================================
// 集成：熔断 + 降级 → 串起来测
// =============================================================================

func TestCircuitBreaker_WithGroup(t *testing.T) {
	// 创建一个没有 peers 的 Group
	// getFromPeer 不会被调用，所有请求直接走 getLocal（降级）
	g := NewGroup("cb-test", 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("local-" + key), nil
		},
	))

	// 不注册任何 peers → PickPeer 返回 false → 始终走 getLocal
	// 验证即便没有远程节点，降级也能正常工作
	v, err := g.Get("foo")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if v.String() != "local-foo" {
		t.Fatalf("expected local-foo, got %s", v.String())
	}

	// breaker 应该还是 Closed（从来没被调用过）
	if g.breaker.IsOpen() {
		t.Fatal("breaker should be closed, getFromPeer was never called")
	}
}
