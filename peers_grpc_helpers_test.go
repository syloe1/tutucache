package geecache

import (
	"net"
	"testing"
	"time"
)

// startTestGRPCServerPool 在 127.0.0.1 的临时端口上启动一个 GRPCPool。
// configure 在 ServeListener 之前执行，用于设置 token/TLS 等只能启动前生效的配置。
// 返回的地址是实际绑定的 host:port（端口为 0 时由内核分配）。
func startTestGRPCServerPool(t *testing.T, configure func(*GRPCPool)) (*GRPCPool, string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	p := NewGRPCPool(addr)
	if configure != nil {
		configure(p)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.ServeListener(ln)
	}()
	waitGRPCServerStarted(t, p)

	t.Cleanup(func() {
		p.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("ServeListener(%s): %v", addr, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("ServeListener(%s) did not return after Stop", addr)
		}
	})

	return p, addr
}

// waitGRPCServerStarted 等待后台 ServeListener 完成 server 创建。
// ServeListener 在调用方 goroutine 之外执行，测试在检查 started（例如
// EnableTLS 必须失败）或发起 RPC 前需要先等到它真正启动。
func waitGRPCServerStarted(t *testing.T, p *GRPCPool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		started := p.started
		p.mu.Unlock()
		if started {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("gRPC server did not start within 2s")
}

// newTestGRPCClientPool 创建一个只连接指定 peers 的客户端池。
// self 使用一个不会被 dial 的地址，避免与 peer 混淆。
func newTestGRPCClientPool(t *testing.T, configure func(*GRPCPool), peers ...string) *GRPCPool {
	t.Helper()

	p := NewGRPCPool("127.0.0.1:1")
	if configure != nil {
		configure(p)
	}
	p.Set(peers...)
	t.Cleanup(p.Stop)
	return p
}

// testGRPCGetter 返回客户端池中指定 peer 的 getter。
func testGRPCGetter(t *testing.T, p *GRPCPool, peer string) *grpcGetter {
	t.Helper()
	p.mu.Lock()
	getter := p.grpcGetters[peer]
	p.mu.Unlock()
	if getter == nil {
		t.Fatalf("no grpc getter for %s (have %v)", peer, getterAddrs(p))
	}
	return getter
}

func getterAddrs(p *GRPCPool) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	addrs := make([]string, 0, len(p.grpcGetters))
	for addr := range p.grpcGetters {
		addrs = append(addrs, addr)
	}
	return addrs
}
