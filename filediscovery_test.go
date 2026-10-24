package geecache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func readPeersFile(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read peers file: %v", err)
	}
	var peers []string
	if err := json.Unmarshal(data, &peers); err != nil {
		t.Fatalf("unmarshal peers file %q: %v", data, err)
	}
	return peers
}

// TestFileDiscoveryConcurrentRegister 验证同一进程内并发注册不会丢节点。
func TestFileDiscoveryConcurrentRegister(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	const n = 8
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			addr := fmt.Sprintf("http://localhost:%d", 8001+i)
			errCh <- NewFileDiscovery(addr, path).Register(addr)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	peers := readPeersFile(t, path)
	if len(peers) != n {
		t.Fatalf("want %d peers, got %d: %v", n, len(peers), peers)
	}
	if !sort.StringsAreSorted(peers) {
		t.Fatalf("peers should be sorted, got %v", peers)
	}
}

// TestFileDiscoveryRegisterHelper 是跨进程测试的子进程入口。
func TestFileDiscoveryRegisterHelper(t *testing.T) {
	path := os.Getenv("TUTUCACHE_PEERS_FILE")
	addr := os.Getenv("TUTUCACHE_PEER_ADDR")
	if path == "" || addr == "" {
		t.Skip("helper process only")
	}
	if err := NewFileDiscovery(addr, path).Register(addr); err != nil {
		t.Fatalf("register %s: %v", addr, err)
	}
}

// TestFileDiscoveryConcurrentRegisterProcesses 同时启动多个进程注册，
// 验证跨进程文件锁确实消除了 lost-update。
func TestFileDiscoveryConcurrentRegisterProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess test in short mode")
	}
	path := filepath.Join(t.TempDir(), "peers.json")

	const n = 5
	type result struct {
		addr string
		out  []byte
		err  error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("http://localhost:%d", 8001+i)
		cmd := exec.Command(os.Args[0],
			"-test.run=^TestFileDiscoveryRegisterHelper$",
			"-test.count=1",
		)
		cmd.Env = append(os.Environ(),
			"TUTUCACHE_PEERS_FILE="+path,
			"TUTUCACHE_PEER_ADDR="+addr,
		)
		go func(addr string) {
			out, err := cmd.CombinedOutput()
			results <- result{addr: addr, out: out, err: err}
		}(addr)
	}
	for i := 0; i < n; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("helper %s failed: %v\n%s", r.addr, r.err, r.out)
		}
	}

	peers := readPeersFile(t, path)
	if len(peers) != n {
		t.Fatalf("want %d peers, got %d: %v", n, len(peers), peers)
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("http://localhost:%d", 8001+i)
		if !contains(peers, want) {
			t.Fatalf("missing %s in %v", want, peers)
		}
	}
}

// TestFileDiscoveryRegisterRejectsCorruptFile 验证读失败时不会用空列表覆盖文件。
func TestFileDiscoveryRegisterRejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	const corrupt = `{"broken":`
	if err := os.WriteFile(path, []byte(corrupt), 0o644); err != nil {
		t.Fatal(err)
	}

	d := NewFileDiscovery("http://localhost:8001", path)
	if err := d.Register(d.self); err == nil {
		t.Fatal("Register should fail when peers file is corrupt")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != corrupt {
		t.Fatalf("corrupt file was overwritten: %q", got)
	}
}

// TestFileDiscoveryWatchReRegisters 验证心跳能把被外部覆盖掉的本节点重新写回。
func TestFileDiscoveryWatchReRegisters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	self := "http://localhost:8001"

	d := NewFileDiscovery(self, path)
	if err := d.Register(self); err != nil {
		t.Fatal(err)
	}
	// 模拟文件被外部覆盖：本节点从列表里消失。
	if err := os.WriteFile(path, []byte(`["http://localhost:8002"]`), 0o644); err != nil {
		t.Fatal(err)
	}

	d.heartbeatInterval = 20 * time.Millisecond
	d.Watch(func([]string) {}) // onChange 不需要断言，这里只验证文件自愈
	defer d.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		peers := readPeersFile(t, path)
		if contains(peers, self) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("self was not re-registered within timeout: %v", readPeersFile(t, path))
}

// TestFileDiscoveryAtomicWrite 验证并发读写时不会读到半截 JSON。
func TestFileDiscoveryAtomicWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	d := NewFileDiscovery("http://localhost:8001", path)
	if err := d.Register(d.self); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 8)

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				addr := fmt.Sprintf("http://localhost:%d", 9000+i*100+j)
				if err := d.Register(addr); err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if _, err := d.GetPeers(); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent access: %v", err)
	}
}

// TestFileDiscoveryUnregister 验证只删除目标地址，并保持有序。
func TestFileDiscoveryUnregister(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	d := NewFileDiscovery("http://localhost:8001", path)

	for _, addr := range []string{
		"http://localhost:8003",
		"http://localhost:8001",
		"http://localhost:8002",
	} {
		if err := d.Register(addr); err != nil {
			t.Fatalf("register %s: %v", addr, err)
		}
	}

	if err := d.Unregister("http://localhost:8002"); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	got := readPeersFile(t, path)
	want := []string{"http://localhost:8001", "http://localhost:8003"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("peers=%v, want %v", got, want)
	}
}

// TestFileDiscoveryUnregisterIdempotent 验证重复注销和注销不存在的地址都是 no-op。
func TestFileDiscoveryUnregisterIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	self := "http://localhost:8001"
	d := NewFileDiscovery(self, path)

	if err := d.Register(self); err != nil {
		t.Fatal(err)
	}
	if err := d.Unregister(self); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if peers := readPeersFile(t, path); len(peers) != 0 {
		t.Fatalf("peers after unregister=%v, want empty", peers)
	}

	// 已经移除的地址、以及从未注册的地址，都应该是 no-op。
	if err := d.Unregister(self); err != nil {
		t.Fatalf("repeat unregister: %v", err)
	}
	if err := d.Unregister("http://localhost:9999"); err != nil {
		t.Fatalf("unregister unknown addr: %v", err)
	}
}

// TestFileDiscoveryUnregisterMissingFile 验证文件不存在时视为已经注销。
func TestFileDiscoveryUnregisterMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	d := NewFileDiscovery("http://localhost:8001", path)

	if err := d.Unregister(d.self); err != nil {
		t.Fatalf("unregister with missing file: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Unregister created a peers file: stat err=%v", err)
	}
}

// TestFileDiscoveryUnregisterRejectsCorruptFile 验证读失败时不会覆盖损坏的文件。
func TestFileDiscoveryUnregisterRejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	const corrupt = `{"broken":`
	if err := os.WriteFile(path, []byte(corrupt), 0o644); err != nil {
		t.Fatal(err)
	}

	d := NewFileDiscovery("http://localhost:8001", path)
	if err := d.Unregister(d.self); err == nil {
		t.Fatal("Unregister should fail when peers file is corrupt")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != corrupt {
		t.Fatalf("corrupt file was overwritten: %q", got)
	}
}

// TestFileDiscoveryConcurrentRegisterUnregister 验证注销不会丢掉并发注册的节点。
func TestFileDiscoveryConcurrentRegisterUnregister(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	self := "http://localhost:8001"
	d := NewFileDiscovery(self, path)

	if err := d.Register(self); err != nil {
		t.Fatal(err)
	}

	const n = 8
	var wg sync.WaitGroup
	errCh := make(chan error, n+1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- d.Unregister(self)
	}()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			addr := fmt.Sprintf("http://localhost:%d", 9001+i)
			errCh <- d.Register(addr)
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent register/unregister: %v", err)
		}
	}

	peers := readPeersFile(t, path)
	if contains(peers, self) {
		t.Fatalf("unregistered node still present: %v", peers)
	}
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("http://localhost:%d", 9001+i)
		if !contains(peers, addr) {
			t.Fatalf("missing %s in %v", addr, peers)
		}
	}
}

// TestFileDiscoveryCloseIdempotent 验证 Watch 从未启动时 Close 不会阻塞，且可重复调用。
func TestFileDiscoveryCloseIdempotent(t *testing.T) {
	d := NewFileDiscovery("http://localhost:8001", filepath.Join(t.TempDir(), "peers.json"))

	done := make(chan struct{})
	go func() {
		d.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked when Watch was never started")
	}

	d.Close()
}

// TestFileDiscoveryCloseStopsWatchUntilDone 验证 Close 会等待 watch 退出，
// 并且之后心跳不会再把自己写回 peers.json。
func TestFileDiscoveryCloseStopsWatchUntilDone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	self := "http://localhost:8001"
	d := NewFileDiscovery(self, path)
	if err := d.Register(self); err != nil {
		t.Fatal(err)
	}
	d.heartbeatInterval = 10 * time.Millisecond

	started := make(chan struct{})
	d.Watch(func([]string) {
		select {
		case <-started:
		default:
			close(started)
		}
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not start")
	}

	d.Close()

	// Close 返回时 watch goroutine 必须已经退出。
	select {
	case <-d.watchDone:
	default:
		t.Fatal("Close returned before watch goroutine exited")
	}

	if err := d.Unregister(self); err != nil {
		t.Fatalf("unregister after Close: %v", err)
	}
	// 如果心跳没有停止，它会在若干个周期后把 self 写回。
	time.Sleep(5 * d.heartbeatInterval)
	if peers := readPeersFile(t, path); contains(peers, self) {
		t.Fatalf("heartbeat re-registered %s after Close: %v", self, peers)
	}
}

// TestFileDiscoveryWatchAfterClose 验证 Close 之后启动的 Watch 不会触发回调。
func TestFileDiscoveryWatchAfterClose(t *testing.T) {
	d := NewFileDiscovery("http://localhost:8001", filepath.Join(t.TempDir(), "peers.json"))
	d.Close()

	called := make(chan struct{}, 1)
	d.Watch(func([]string) { called <- struct{}{} })
	select {
	case <-called:
		t.Fatal("Watch callback ran after Close")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestFileDiscoveryWatchStartsOnce 验证重复调用 Watch 不会启动多个 goroutine。
func TestFileDiscoveryWatchStartsOnce(t *testing.T) {
	d := NewFileDiscovery("http://localhost:8001", filepath.Join(t.TempDir(), "peers.json"))
	if err := d.Register(d.self); err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	first := make(chan struct{}, 1)
	second := make(chan struct{}, 1)
	d.Watch(func([]string) { first <- struct{}{} })
	d.Watch(func([]string) { second <- struct{}{} })

	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("first Watch did not start")
	}
	select {
	case <-second:
		t.Fatal("second Watch started another goroutine")
	case <-time.After(50 * time.Millisecond):
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
