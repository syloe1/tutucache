package geecache

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "geecache/geecachepb"

	"google.golang.org/protobuf/proto"
)

// breakerSnapshot 在锁内读取熔断器状态，避免测试本身引入数据竞争。
func breakerSnapshot(cb *CircuitBreaker) (state State, failures int, probe bool, generation uint64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state, len(cb.failureTimes), cb.probeInFlight, cb.generation
}

func mustAllow(t *testing.T, cb *CircuitBreaker) Ticket {
	t.Helper()
	ticket, ok := cb.Allow()
	if !ok {
		t.Fatal("expected request to be admitted")
	}
	return ticket
}

func TestCircuitBreaker_ClosedToOpen(t *testing.T) {
	cb := NewCircuitBreaker(2, 5*time.Second)

	// 第 1 次失败：仍在 Closed，计数为 1
	cb.RecordFailure(mustAllow(t, cb))
	if state, failures, _, _ := breakerSnapshot(cb); state != StateClosed || failures != 1 {
		t.Fatalf("after 1 failure: state=%v failures=%d, want closed/1", state, failures)
	}

	// 第 2 次失败：达到阈值，进入 Open
	cb.RecordFailure(mustAllow(t, cb))
	if state, _, _, _ := breakerSnapshot(cb); state != StateOpen {
		t.Fatalf("after 2 failures: state=%v, want open", state)
	}

	// Open 且冷却未到 → 拒绝准入
	if _, ok := cb.Allow(); ok {
		t.Fatal("open breaker should reject requests")
	}
}

func TestCircuitBreaker_ClosedSuccessResetsFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(2, time.Hour)

	cb.RecordFailure(mustAllow(t, cb))
	cb.RecordSuccess(mustAllow(t, cb)) // 重置连续失败计数
	cb.RecordFailure(mustAllow(t, cb))

	if state, failures, _, _ := breakerSnapshot(cb); state != StateClosed || failures != 1 {
		t.Fatalf("after fail/success/fail: state=%v failures=%d, want closed/1", state, failures)
	}

	// 再来一次失败才达到阈值
	cb.RecordFailure(mustAllow(t, cb))
	if state, _, _, _ := breakerSnapshot(cb); state != StateOpen {
		t.Fatalf("state=%v, want open after 2 consecutive failures", state)
	}
}

// 回归 #17：只有滑动窗口内的失败才计数。三次失败如果间隔都超过窗口，
// 不能因为「历史上累计三次」而熔断。
func TestCircuitBreaker_FailuresOutsideWindowDoNotTrip(t *testing.T) {
	cb := newCircuitBreaker(3, time.Hour, 30*time.Millisecond)

	for i := 0; i < 3; i++ {
		cb.RecordFailure(mustAllow(t, cb))
		if i < 2 {
			time.Sleep(60 * time.Millisecond) // 超过窗口，之前的失败应被丢弃
		}
	}

	state, failures, _, _ := breakerSnapshot(cb)
	if state != StateClosed || failures != 1 {
		t.Fatalf("after spaced failures: state=%v failures=%d, want closed/1", state, failures)
	}
}

// 回归 #17：滑动窗口约束的是总跨度，而不是相邻两次失败的间隔。
// 第一次失败已经滑出窗口时，即使相邻间隔都小于窗口，也不能熔断。
func TestCircuitBreaker_WindowSpansNotChainedGaps(t *testing.T) {
	cb := newCircuitBreaker(3, time.Hour, 50*time.Millisecond)

	cb.RecordFailure(mustAllow(t, cb))
	time.Sleep(30 * time.Millisecond)
	cb.RecordFailure(mustAllow(t, cb))
	time.Sleep(30 * time.Millisecond)
	cb.RecordFailure(mustAllow(t, cb))

	// 第三次失败时，第一次失败已超过 50ms 窗口，最多只剩两次计数。
	if state, _, _, _ := breakerSnapshot(cb); state != StateClosed {
		t.Fatalf("state=%v, want closed: first failure should have expired", state)
	}
}

// 窗口内的连续失败仍然要能正常熔断。
func TestCircuitBreaker_FailuresWithinWindowTrip(t *testing.T) {
	cb := newCircuitBreaker(3, time.Hour, time.Second)

	for i := 0; i < 3; i++ {
		cb.RecordFailure(mustAllow(t, cb))
	}

	if state, failures, _, _ := breakerSnapshot(cb); state != StateOpen || failures != 3 {
		t.Fatalf("after 3 failures within window: state=%v failures=%d, want open/3", state, failures)
	}
}

// pruneFailureTimes 的边界：恰好等于窗口的失败记录仍然有效，
// 只有严格早于 cutoff 的记录才会被丢弃。
func TestCircuitBreaker_PruneFailureTimesBoundary(t *testing.T) {
	cb := newCircuitBreaker(3, time.Hour, 30*time.Second)
	now := time.Now()
	cb.failureTimes = []time.Time{
		now.Add(-31 * time.Second), // 滑出窗口
		now.Add(-30 * time.Second), // 恰好等于窗口边界，保留
		now.Add(-time.Second),
	}

	cb.pruneFailureTimes(now)

	if got := len(cb.failureTimes); got != 2 {
		t.Fatalf("remaining failures = %d, want 2", got)
	}
}

// 窗口配置为 0 或负数时退化为累计计数，避免配置错误让熔断器永远无法打开。
func TestCircuitBreaker_NonPositiveWindowKeepsCumulativeCount(t *testing.T) {
	cb := newCircuitBreaker(2, time.Hour, 0)

	cb.RecordFailure(mustAllow(t, cb))
	time.Sleep(10 * time.Millisecond)
	cb.RecordFailure(mustAllow(t, cb))

	if state, _, _, _ := breakerSnapshot(cb); state != StateOpen {
		t.Fatalf("state=%v, want open with non-positive window", state)
	}
}

func TestCircuitBreaker_OpenToHalfOpenReservesOneProbe(t *testing.T) {
	cb := NewCircuitBreaker(1, 30*time.Millisecond)

	cb.RecordFailure(mustAllow(t, cb))
	_, _, _, genBefore := breakerSnapshot(cb)

	if _, ok := cb.Allow(); ok {
		t.Fatal("open breaker should reject during cooldown")
	}

	time.Sleep(50 * time.Millisecond)

	// 冷却到期：第一个请求成为探测，占住唯一名额
	probe := mustAllow(t, cb)
	state, _, inFlight, genAfter := breakerSnapshot(cb)
	if state != StateHalfOpen || !inFlight || genAfter != genBefore+1 {
		t.Fatalf("after cooldown: state=%v probeInFlight=%v gen=%d, want half-open/true/%d",
			state, inFlight, genAfter, genBefore+1)
	}

	// 半开期间的其他请求必须被拒绝
	if _, ok := cb.Allow(); ok {
		t.Fatal("half-open must admit only one probe")
	}

	// 探测成功 → Closed，名额释放，计数清零
	cb.RecordSuccess(probe)
	if state, failures, inFlight, _ := breakerSnapshot(cb); state != StateClosed || failures != 0 || inFlight {
		t.Fatalf("after probe success: state=%v failures=%d probeInFlight=%v, want closed/0/false",
			state, failures, inFlight)
	}
}

func TestCircuitBreaker_HalfOpenSuccess(t *testing.T) {
	cb := NewCircuitBreaker(2, 30*time.Millisecond)

	cb.RecordFailure(mustAllow(t, cb))
	cb.RecordFailure(mustAllow(t, cb))
	if state, _, _, _ := breakerSnapshot(cb); state != StateOpen {
		t.Fatalf("state=%v, want open", state)
	}

	time.Sleep(50 * time.Millisecond)
	probe := mustAllow(t, cb)
	cb.RecordSuccess(probe)

	// 计数已清零：一次失败不会立即熔断
	cb.RecordFailure(mustAllow(t, cb))
	if state, failures, _, _ := breakerSnapshot(cb); state != StateClosed || failures != 1 {
		t.Fatalf("after recovery: state=%v failures=%d, want closed/1", state, failures)
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(1, 30*time.Millisecond)

	cb.RecordFailure(mustAllow(t, cb))
	time.Sleep(50 * time.Millisecond)

	probe := mustAllow(t, cb)
	cb.RecordFailure(probe)

	// 探测失败 → 回到 Open，并释放名额
	state, _, inFlight, _ := breakerSnapshot(cb)
	if state != StateOpen || inFlight {
		t.Fatalf("after probe failure: state=%v probeInFlight=%v, want open/false", state, inFlight)
	}

	// 冷却必须从探测失败时刻重新计算
	if _, ok := cb.Allow(); ok {
		t.Fatal("cooldown should restart after probe failure")
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := cb.Allow(); !ok {
		t.Fatal("expected a new probe after the restarted cooldown")
	}
}

func TestCircuitBreaker_StaleSuccessDoesNotCloseOpen(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Hour)

	// stale 是 Closed 状态下获准的旧请求
	stale := mustAllow(t, cb)

	// 其他请求连续失败，达到阈值进入 Open
	for i := 0; i < 3; i++ {
		cb.RecordFailure(mustAllow(t, cb))
	}
	if state, _, _, _ := breakerSnapshot(cb); state != StateOpen {
		t.Fatalf("state=%v, want open after threshold failures", state)
	}
	_, failuresBefore, _, genBefore := breakerSnapshot(cb)

	// 旧请求此时才成功，必须整体作废
	cb.RecordSuccess(stale)

	state, failuresAfter, inFlight, genAfter := breakerSnapshot(cb)
	if state != StateOpen || failuresAfter != failuresBefore || inFlight || genAfter != genBefore {
		t.Fatalf("stale success changed state: state=%v failures=%d gen=%d, want open/%d/%d",
			state, failuresAfter, genAfter, failuresBefore, genBefore)
	}
}

func TestCircuitBreaker_StaleFailureDoesNotExtendCooldown(t *testing.T) {
	cb := NewCircuitBreaker(1, 30*time.Millisecond)

	stale := mustAllow(t, cb)
	cb.RecordFailure(mustAllow(t, cb)) // 进入 Open
	time.Sleep(50 * time.Millisecond)

	// 若被接受，会刷新 lastFailure 并让下面的 Allow 继续拒绝
	cb.RecordFailure(stale)

	if _, ok := cb.Allow(); !ok {
		t.Fatal("stale failure must not extend cooldown")
	}
}

func TestCircuitBreaker_StaleFailureDoesNotCountAfterRecovery(t *testing.T) {
	cb := NewCircuitBreaker(1, 30*time.Millisecond)

	stale := mustAllow(t, cb)
	cb.RecordFailure(mustAllow(t, cb))
	time.Sleep(50 * time.Millisecond)

	probe := mustAllow(t, cb)
	cb.RecordSuccess(probe) // 恢复为 Closed，代数再次 +1

	cb.RecordFailure(stale) // 旧代数结果，不能把 Closed 再次打回 Open

	if state, failures, _, _ := breakerSnapshot(cb); state != StateClosed || failures != 0 {
		t.Fatalf("stale failure changed recovered breaker: state=%v failures=%d, want closed/0",
			state, failures)
	}
}

func TestCircuitBreaker_DuplicateProbeCallbackIgnored(t *testing.T) {
	cb := NewCircuitBreaker(1, 30*time.Millisecond)

	cb.RecordFailure(mustAllow(t, cb))
	time.Sleep(50 * time.Millisecond)
	probe := mustAllow(t, cb)
	cb.RecordSuccess(probe)

	_, _, _, genBefore := breakerSnapshot(cb)

	// 同一个探测票重复回填：第一次结果已迁移状态，后续必须作废
	cb.RecordFailure(probe)
	cb.RecordSuccess(probe)

	if state, failures, inFlight, genAfter := breakerSnapshot(cb); state != StateClosed || failures != 0 || inFlight || genAfter != genBefore {
		t.Fatalf("duplicate probe callback changed state: state=%v failures=%d gen=%d, want closed/0/%d",
			state, failures, genAfter, genBefore)
	}
}

func TestCircuitBreaker_ZeroTicketIgnored(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Hour)

	cb.RecordSuccess(Ticket{})
	cb.RecordFailure(Ticket{})

	if state, failures, inFlight, generation := breakerSnapshot(cb); state != StateClosed || failures != 0 || inFlight || generation != 1 {
		t.Fatalf("zero ticket changed state: state=%v failures=%d gen=%d, want closed/0/1",
			state, failures, generation)
	}
}

func TestCircuitBreaker_ConcurrentFailuresTripOnce(t *testing.T) {
	const (
		thresh  = 5
		workers = 100
	)
	cb := NewCircuitBreaker(thresh, time.Hour)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if ticket, ok := cb.Allow(); ok {
				cb.RecordFailure(ticket)
			}
		}()
	}
	wg.Wait()

	// 只有触发熔断的那一代失败被计数；其余在途结果全部失效
	state, failures, _, generation := breakerSnapshot(cb)
	if state != StateOpen {
		t.Fatalf("state=%v, want open", state)
	}
	if failures != thresh {
		t.Fatalf("failures=%d, want exactly %d", failures, thresh)
	}
	if generation != 2 {
		t.Fatalf("generation=%d, want 2 (one closed->open transition)", generation)
	}
}

func TestCircuitBreaker_ConcurrentHalfOpenAdmitsOneProbe(t *testing.T) {
	cb := NewCircuitBreaker(1, 20*time.Millisecond)
	cb.RecordFailure(mustAllow(t, cb))
	time.Sleep(50 * time.Millisecond)

	const workers = 50
	admitted := make(chan Ticket, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if ticket, ok := cb.Allow(); ok {
				admitted <- ticket
			}
		}()
	}
	wg.Wait()
	close(admitted)

	if got := len(admitted); got != 1 {
		t.Fatalf("half-open admitted %d probes, want exactly 1", got)
	}

	probe := <-admitted
	if state, _, inFlight, _ := breakerSnapshot(cb); state != StateHalfOpen || !inFlight {
		t.Fatalf("state=%v probeInFlight=%v, want half-open/true", state, inFlight)
	}

	cb.RecordSuccess(probe)
	if state, _, inFlight, _ := breakerSnapshot(cb); state != StateClosed || inFlight {
		t.Fatalf("state=%v probeInFlight=%v, want closed/false", state, inFlight)
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
	if state, _, _, _ := breakerSnapshot(g.breaker); state != StateClosed {
		t.Fatalf("breaker state=%v, want closed; getFromPeer was never called", state)
	}
}

type failingPeer struct {
	calls atomic.Int32
}

func (p *failingPeer) Get(*pb.Request, *pb.Response) error {
	p.calls.Add(1)
	return errors.New("peer down")
}

func TestCircuitBreaker_GetFromPeerFastFails(t *testing.T) {
	g := NewGroup("cb-peer-test", 1<<20, GetterFunc(
		func(key string) ([]byte, error) { return []byte("local-" + key), nil },
	))
	peer := &failingPeer{}

	// NewGroup 的阈值为 3：前三次真实请求失败并计数
	for i := 0; i < 3; i++ {
		if _, err := g.getFromPeer(peer, "foo"); err == nil {
			t.Fatalf("call %d: expected peer error", i+1)
		}
	}

	// 第四次被熔断器快速拒绝，不再触达 peer
	if _, err := g.getFromPeer(peer, "foo"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if got := peer.calls.Load(); got != 3 {
		t.Fatalf("peer called %d times, want 3 (fourth rejected by breaker)", got)
	}
}

// 回归 #14：熔断器必须按 peer 隔离。一个坏节点的连续失败只能熔断它自己，
// 不能把同一 Group 内路由到健康节点的请求一起短路。
func TestCircuitBreakerIsPerPeer(t *testing.T) {
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badSrv.Close()
	goodSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body, err := proto.Marshal(&pb.Response{Value: []byte("ok")})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(body)
	}))
	defer goodSrv.Close()

	g := NewGroup("cb-per-peer", 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("local"), nil
	}))
	bad := newHTTPGetter(badSrv.URL+defaultBasePath, nil, "")
	good := newHTTPGetter(goodSrv.URL+defaultBasePath, nil, "")

	for i := 0; i < defaultBreakerFailureThreshold; i++ {
		if _, err := g.getFromPeer(bad, "k"); err == nil {
			t.Fatalf("bad peer call %d: expected error", i+1)
		}
	}
	if _, err := g.getFromPeer(bad, "k"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("bad peer breaker should be open, got %v", err)
	}

	// 健康 peer 的熔断器必须仍然是 Closed。
	v, err := g.getFromPeer(good, "k")
	if err != nil {
		t.Fatalf("healthy peer short-circuited by another peer's breaker: %v", err)
	}
	if v.String() != "ok" {
		t.Fatalf("healthy peer value = %q, want %q", v.String(), "ok")
	}
}

// 回归 #14：同一个 peer 在不同 Group 下的熔断状态也必须相互独立。
func TestCircuitBreakerIsPerGroupForSamePeer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	getter := newHTTPGetter(srv.URL+defaultBasePath, nil, "")
	g1 := NewGroup("cb-group-a", 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("a"), nil
	}))
	g2 := NewGroup("cb-group-b", 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("b"), nil
	}))

	for i := 0; i < defaultBreakerFailureThreshold; i++ {
		if _, err := g1.getFromPeer(getter, "k"); err == nil {
			t.Fatalf("group a call %d: expected error", i+1)
		}
	}
	if _, err := g1.getFromPeer(getter, "k"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("group a breaker should be open, got %v", err)
	}

	// g2 仍然应该真实访问 peer（拿到 500），而不是被 g1 的熔断器短路。
	_, err := g2.getFromPeer(getter, "k")
	if errors.Is(err, ErrCircuitOpen) {
		t.Fatal("group b was short-circuited by group a's breaker")
	}
	var peerErr *PeerHTTPError
	if !errors.As(err, &peerErr) || peerErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("group b error = %v, want HTTP 500", err)
	}
}

// 回归 #14：peer 下线后 getter 被丢弃，其熔断状态也必须随之回收；
// 节点重新上线时应使用全新的熔断器。
func TestPeerBreakerDroppedWhenPeerRemoved(t *testing.T) {
	pool := NewHTTPPool("http://self")
	pool.Set("http://peer-a")

	pool.mu.Lock()
	first := pool.httpGetters["http://peer-a"]
	pool.mu.Unlock()
	if first == nil {
		t.Fatal("peer getter was not created")
	}
	firstBreaker := first.BreakerFor("some-group")

	pool.Set()                // 节点下线
	pool.Set("http://peer-a") // 重新上线 → 新 getter

	pool.mu.Lock()
	second := pool.httpGetters["http://peer-a"]
	pool.mu.Unlock()
	if second == nil || second == first {
		t.Fatal("expected a fresh getter after peer removal")
	}
	if cb := second.BreakerFor("some-group"); cb == firstBreaker {
		t.Fatal("breaker state leaked across peer removal")
	}
}
