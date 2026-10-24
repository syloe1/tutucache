package geecache

import (
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/connectivity"
)

// fakeDiscovery 记录注册调用，并允许测试手动触发节点变更回调。
type fakeDiscovery struct {
	mu           sync.Mutex
	registered   []string
	unregistered []string
	onChange     func([]string)
	registerErr  error
}

func (f *fakeDiscovery) GetPeers() ([]string, error) { return nil, nil }

func (f *fakeDiscovery) Register(addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registerErr != nil {
		return f.registerErr
	}
	f.registered = append(f.registered, addr)
	return nil
}

func (f *fakeDiscovery) Unregister(addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unregistered = append(f.unregistered, addr)
	return nil
}

func (f *fakeDiscovery) Watch(onChange func([]string)) {
	f.mu.Lock()
	f.onChange = onChange
	f.mu.Unlock()
}

func (f *fakeDiscovery) update(peers []string) {
	f.mu.Lock()
	onChange := f.onChange
	f.mu.Unlock()
	if onChange != nil {
		onChange(peers)
	}
}

func (f *fakeDiscovery) registeredAddrs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.registered...)
}

// StartDiscovery 注册归一化后的 self，并在回调中按 gRPC 地址规则更新拓扑。
func TestGRPCStartDiscovery(t *testing.T) {
	d := &fakeDiscovery{}
	p := NewGRPCPool("http://localhost:8001")
	defer p.Stop()

	if err := p.StartDiscovery(d); err != nil {
		t.Fatalf("StartDiscovery: %v", err)
	}
	if got := d.registeredAddrs(); !sameSlice(got, []string{"localhost:8001"}) {
		t.Fatalf("registered = %v, want [localhost:8001]", got)
	}

	// peers.json 中混有 HTTP URL 和裸地址；gRPC 将它们统一归一化为 host:port。
	d.update([]string{
		"http://localhost:8001", // self
		"http://localhost:8002",
		"localhost:8003", // 与下面重复
		"https://localhost:8003/_geecache/",
	})

	wantPeers := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	if !sameSlice(p.peersList, wantPeers) {
		t.Fatalf("peersList = %v, want %v", p.peersList, wantPeers)
	}
	removed := p.grpcGetters["localhost:8002"]
	if removed == nil {
		t.Fatal("missing getter for localhost:8002")
	}
	if len(p.grpcGetters) != 2 {
		t.Fatalf("grpcGetters = %d, want 2", len(p.grpcGetters))
	}

	// 节点下线：连接必须关闭。
	d.update([]string{"localhost:8003"})
	if _, ok := p.grpcGetters["localhost:8002"]; ok {
		t.Fatal("removed peer localhost:8002 still has a getter")
	}
	if got := removed.conn.GetState(); got != connectivity.Shutdown {
		t.Fatalf("removed peer conn state = %v, want Shutdown", got)
	}

	// Stop 之后 discovery 的回调仍可能到达，但不能再建立新连接。
	p.Stop()
	d.update([]string{"localhost:8004"})
	if len(p.grpcGetters) != 0 {
		t.Fatalf("Set after Stop created %d getters, want 0", len(p.grpcGetters))
	}
}

func TestGRPCStartDiscoveryRegistrationError(t *testing.T) {
	wantErr := errors.New("registry unavailable")
	d := &fakeDiscovery{registerErr: wantErr}
	p := NewGRPCPool("localhost:8001")
	defer p.Stop()

	if err := p.StartDiscovery(d); !errors.Is(err, wantErr) {
		t.Fatalf("StartDiscovery error = %v, want %v", err, wantErr)
	}
}
