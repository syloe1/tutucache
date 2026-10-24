package geecache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	pb "geecache/geecachepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTokenEqual(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
		ok   bool
	}{
		{"both empty", "", "", true},
		{"exact", "secret", "secret", true},
		{"different last byte", "secreT", "secret", false},
		{"prefix only", "secre", "secret", false},
		{"longer", "secret-longer", "secret", false},
		{"empty vs set", "", "secret", false},
		{"set vs empty", "secret", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tokenEqual(tt.got, tt.want); got != tt.ok {
				t.Fatalf("tokenEqual(%q, %q) = %v, want %v", tt.got, tt.want, got, tt.ok)
			}
		})
	}
}

func TestNormalizeGRPCPeer(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"bare host port", "localhost:8001", "localhost:8001"},
		{"trim spaces", "  localhost:8001  ", "localhost:8001"},
		{"http url", "http://localhost:8001", "localhost:8001"},
		{"https url with path", "https://localhost:8001/_geecache/", "localhost:8001"},
		{"grpc url", "grpc://localhost:8001", "localhost:8001"},
		{"ipv4", "127.0.0.1:8001", "127.0.0.1:8001"},
		{"ipv6 bare", "[::1]:8001", "[::1]:8001"},
		{"ipv6 url", "http://[::1]:8001", "[::1]:8001"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeGRPCPeer(tt.in); got != tt.want {
				t.Fatalf("normalizeGRPCPeer(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// self 与 peer 都必须归一化为裸 host:port，且 URL 形式的重复 peer 只保留一份。
func TestGRPCPoolNormalizesSelfAndPeers(t *testing.T) {
	p := NewGRPCPool("http://localhost:8001")
	defer p.Stop()

	p.Set(
		"http://localhost:8001", // self，应被跳过
		"http://localhost:8002",
		"localhost:8002", // 归一化后与上一项重复
		"https://localhost:8003/_geecache/",
	)

	if p.self != "localhost:8001" {
		t.Errorf("self = %q, want localhost:8001", p.self)
	}
	want := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	if !sameSlice(p.peersList, want) {
		t.Errorf("peersList = %v, want %v", p.peersList, want)
	}
	if len(p.grpcGetters) != 2 {
		t.Errorf("grpcGetters = %d, want 2", len(p.grpcGetters))
	}
	for _, peer := range []string{"localhost:8002", "localhost:8003"} {
		if p.grpcGetters[peer] == nil {
			t.Errorf("missing getter for %s", peer)
		}
	}
	if _, ok := p.grpcGetters["localhost:8001"]; ok {
		t.Error("self must not have a client connection")
	}
}

// 正确 token 成功；缺失或错误 token 返回 Unauthenticated，并被归类为配置错误。
func TestGRPCSharedTokenAuth(t *testing.T) {
	const (
		groupName = "grpc-auth"
		token     = "s3cret-token"
	)
	NewGroup(groupName, 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("remote-value"), nil
	}))

	_, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
		p.SetSharedToken(token)
	})

	tests := []struct {
		name      string
		clientTok string
		wantErr   codes.Code
	}{
		{"correct token", token, codes.OK},
		{"missing token", "", codes.Unauthenticated},
		{"wrong token", "wrong", codes.Unauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var client *GRPCPool
			if tt.clientTok != "" {
				client = newTestGRPCClientPool(t, func(p *GRPCPool) {
					p.SetSharedToken(tt.clientTok)
				}, addr)
			} else {
				client = newTestGRPCClientPool(t, nil, addr)
			}

			getter := testGRPCGetter(t, client, addr)
			out := &pb.Response{}
			err := getter.Get(&pb.Request{Group: groupName, Key: "k"}, out)

			if tt.wantErr == codes.OK {
				if err != nil {
					t.Fatalf("Get() error: %v", err)
				}
				if got := string(out.Value); got != "remote-value" {
					t.Fatalf("value = %q, want remote-value", got)
				}
				return
			}
			if err == nil {
				t.Fatal("Get() succeeded without a valid token")
			}
			if got := status.Code(err); got != tt.wantErr {
				t.Fatalf("code = %v, want %v", got, tt.wantErr)
			}
			if !isPeerConfigError(err) {
				t.Error("authentication failure must be classified as a config error")
			}
		})
	}
}

// 服务端未配置 token 时关闭认证，客户端带不带 token 都可以访问。
func TestGRPCSharedTokenDisabled(t *testing.T) {
	const groupName = "grpc-auth-disabled"
	NewGroup(groupName, 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("open"), nil
	}))

	_, addr := startTestGRPCServerPool(t, nil)
	client := newTestGRPCClientPool(t, func(p *GRPCPool) {
		p.SetSharedToken("ignored-by-server")
	}, addr)

	out := &pb.Response{}
	if err := testGRPCGetter(t, client, addr).Get(&pb.Request{Group: groupName, Key: "k"}, out); err != nil {
		t.Fatalf("Get() with auth disabled: %v", err)
	}
	if got := string(out.Value); got != "open" {
		t.Fatalf("value = %q, want open", got)
	}
}

// token 由 interceptor 每次 RPC 读取：服务端/客户端轮换 token 后立即可用，
// 不需要重连，客户端 getter（及其熔断状态）保持不变。
func TestGRPCSharedTokenRotationWithoutReconnect(t *testing.T) {
	const (
		groupName = "grpc-auth-rotate"
		token1    = "token-one"
		token2    = "token-two"
	)
	NewGroup(groupName, 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("ok"), nil
	}))

	server, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
		p.SetSharedToken(token1)
	})
	client := newTestGRPCClientPool(t, func(p *GRPCPool) {
		p.SetSharedToken(token1)
	}, addr)
	getter := testGRPCGetter(t, client, addr)

	if err := getter.Get(&pb.Request{Group: groupName, Key: "k"}, &pb.Response{}); err != nil {
		t.Fatalf("initial Get() with token1: %v", err)
	}

	// 服务端切换到 token2：同一连接、同一 getter，请求应被拒绝。
	server.SetSharedToken(token2)
	err := getter.Get(&pb.Request{Group: groupName, Key: "k"}, &pb.Response{})
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("after server token rotation code = %v, want Unauthenticated", got)
	}

	// 客户端跟上 token2 后立即恢复，且没有重建 getter/连接。
	client.SetSharedToken(token2)
	if err := getter.Get(&pb.Request{Group: groupName, Key: "k"}, &pb.Response{}); err != nil {
		t.Fatalf("Get() after client token rotation: %v", err)
	}
	if got := testGRPCGetter(t, client, addr); got != getter {
		t.Fatal("token rotation rebuilt the getter/connection")
	}
}

// 认证失败是配置错误：Group.Get 必须把错误返回给调用方，不能静默回退本地。
func TestGRPCGroupAuthFailureDoesNotFallbackLocally(t *testing.T) {
	const groupName = "grpc-auth-no-fallback"
	var localLoads atomic.Int32

	gee := NewGroup(groupName, 1<<20, GetterFunc(func(string) ([]byte, error) {
		localLoads.Add(1)
		return []byte("local"), nil
	}))

	_, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
		p.SetSharedToken("server-token")
	})

	client := newTestGRPCClientPool(t, func(p *GRPCPool) {
		p.SetSharedToken("client-token")
	}, addr)
	gee.RegisterPeers(client)

	if _, err := gee.Get("k"); err == nil {
		t.Fatal("Group.Get() succeeded with mismatched tokens")
	} else if !isPeerConfigError(err) {
		t.Fatalf("Group.Get() error = %v, want config error", err)
	}
	if n := localLoads.Load(); n != 0 {
		t.Fatalf("local getter called %d time(s) after auth failure, want 0", n)
	}
}

func TestGRPCPoolAuthorized(t *testing.T) {
	p := NewGRPCPool("localhost:8001")
	if !p.authorized("") || !p.authorized("anything") {
		t.Fatal("empty token config must disable auth")
	}

	p.SetSharedToken("secret")
	if !p.authorized("secret") {
		t.Error("correct token rejected")
	}
	for _, got := range []string{"", "secre", "secret-longer", "Secret"} {
		if p.authorized(got) {
			t.Errorf("authorized(%q) = true, want false", got)
		}
	}
}

// 配置写入与请求读取并发进行时不能有数据竞争（配合 -race 运行）。
func TestGRPCPoolConfigMutationConcurrentSafe(t *testing.T) {
	p := NewGRPCPool("127.0.0.1:1")
	p.Set("127.0.0.1:2", "127.0.0.1:3")
	defer p.Stop()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p.SetSharedToken(fmt.Sprintf("token-%d", i%2))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p.Set("127.0.0.1:2", "127.0.0.1:3")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = p.authorized("token-0")
			_, _ = p.PickPeer("key")
			_ = p.TLSConfig()
		}
	}()
	wg.Wait()
}
