package geecache

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	pb "geecache/geecachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// =============================================================================
// 辅助函数：创建内存中的 gRPC 连接（不占用真实端口）
// =============================================================================

const bufSize = 1024 * 1024

// startTestGRPCServer 在内存 buffer 上启动 gRPC server，返回 client 连接
func startTestGRPCServer(t *testing.T, server pb.GroupCacheServer) (pb.GroupCacheClient, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterGroupCacheServer(s, server)

	go func() { //异步监听， 等待连接
		if err := s.Serve(lis); err != nil {
			t.Logf("gRPC server exited: %v", err)
		}
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	cleanup := func() {
		conn.Close()
		s.Stop()
	}

	return pb.NewGroupCacheClient(conn), cleanup
}

// =============================================================================
// 测试 GRPCServer.Get
// =============================================================================

func TestGRPCServer_Get_GroupNotFound(t *testing.T) {
	svr := &GRPCServer{}
	// 不创建任何 Group，直接调用 Get
	_, err := svr.Get(context.TODO(), &pb.Request{Group: "nonexistent", Key: "k"})
	if err == nil {
		t.Fatal("expected error for nonexistent group")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", st.Code())
	}
}

func TestGRPCServer_Get_Success(t *testing.T) {
	// 创建带数据的 Group
	loadCount := 0
	NewGroup("test-grpc", 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			loadCount++
			return []byte("hello-" + key), nil
		},
	))

	svr := &GRPCServer{}
	resp, err := svr.Get(context.TODO(), &pb.Request{Group: "test-grpc", Key: "world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(resp.Value) != "hello-world" {
		t.Fatalf("expected 'hello-world', got %q", resp.Value)
	}
	if loadCount != 1 {
		t.Fatalf("expected 1 load, got %d", loadCount)
	}
}

func TestGRPCServer_Get_KeyNotFound(t *testing.T) {
	NewGroup("test-miss", 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			return nil, fmt.Errorf("key %s not exist", key)
		},
	))

	svr := &GRPCServer{}
	_, err := svr.Get(context.TODO(), &pb.Request{Group: "test-miss", Key: "no-such-key"})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

// =============================================================================
// 测试 grpcGetter.Get —— 通过真实 gRPC 连接
// =============================================================================

func TestGrpcGetter_Get(t *testing.T) {
	// 启动真实的 gRPC server（内存中，不占用端口）
	client, cleanup := startTestGRPCServer(t, &GRPCServer{})
	defer cleanup()

	// 创建带数据的 Group
	NewGroup("grpc-getter-test", 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("value-" + key), nil
		},
	))

	getter := &grpcGetter{addr: "bufnet", client: client}

	// 发起请求
	req := &pb.Request{Group: "grpc-getter-test", Key: "foo"}
	resp := &pb.Response{}
	err := getter.Get(req, resp)
	if err != nil {
		t.Fatalf("grpcGetter.Get failed: %v", err)
	}
	if string(resp.Value) != "value-foo" {
		t.Fatalf("expected 'value-foo', got %q", resp.Value)
	}
}

// =============================================================================
// 测试 GRPCPool —— 一致性哈希选节点
// =============================================================================

func TestGRPCPool_PickPeer(t *testing.T) {
	pool := NewGRPCPool("localhost:8001")
	pool.Set(
		"localhost:8001", // self
		"localhost:8002",
		"localhost:8003",
	)

	// 验证至少能选中一个非 self 节点（多试几个 key 避免哈希到 self）
	found := false
	for _, k := range []string{"some-key", "another-key", "key3", "key4", "key5"} {
		if getter, ok := pool.PickPeer(k); ok && getter != nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected to pick at least one non-self peer")
	}
}

func TestGRPCPool_PickPeer_NoPeers(t *testing.T) {
	pool := NewGRPCPool("localhost:8001")
	// 没有调用 Set，peers 为 nil

	if _, ok := pool.PickPeer("any-key"); ok {
		t.Fatal("expected false when no peers configured")
	}
}

// =============================================================================
// 集成测试：完整链路 —— GRPCPool + gRPC Server + Group.Get
// =============================================================================

func TestGRPCIntegration_FullRoundTrip(t *testing.T) {
	// 1. 启动服务端（节点 8002，监听真实端口）
	serverAddr := "localhost:18002"
	serverPool := NewGRPCPool(serverAddr)

	// 创建带业务数据的 Group
	NewGroup("scores", 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			db := map[string]string{"Tom": "630", "Jack": "589"}
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		},
	))

	// 启动 gRPC server
	go func() {
		if err := serverPool.Serve(); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()
	defer serverPool.Stop()
	time.Sleep(100 * time.Millisecond) // 等待 server 就绪

	// 2. 启动客户端（节点 8001，把 8002 作为对端）
	clientPool := NewGRPCPool("localhost:18001")
	clientPool.Set("localhost:18001", "localhost:18002")

	// 3. 客户端通过 PickPeer 找到 server，发起 gRPC 请求
	getter, ok := clientPool.PickPeer("Tom")
	if !ok {
		t.Fatal("expected to pick peer for key Tom")
	}

	req := &pb.Request{Group: "scores", Key: "Tom"}
	resp := &pb.Response{}
	if err := getter.Get(req, resp); err != nil {
		t.Fatalf("gRPC Get failed: %v", err)
	}
	if string(resp.Value) != "630" {
		t.Fatalf("expected '630', got %q", resp.Value)
	}
}

func TestGRPCIntegration_MultiNode(t *testing.T) {
	// 启动 2 个真实 gRPC 服务
	addrs := []string{"localhost:18003", "localhost:18004"}
	pools := make([]*GRPCPool, 2)

	// 创建一个 Group，两个节点共享
	NewGroup("shared", 1<<20, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("shared-" + key), nil
		},
	))

	for i, addr := range addrs {
		pools[i] = NewGRPCPool(addr)
		go func(idx int, a string) {
			if err := pools[idx].Serve(); err != nil {
				t.Logf("server %s stopped: %v", a, err)
			}
		}(i, addr)
		defer pools[i].Stop()
	}
	time.Sleep(200 * time.Millisecond)

	// 配置两个节点的拓扑
	for i := range 2 {
		pools[i].Set(addrs...)
	}

	// 从节点 0 发起请求，一致性哈希决定路由到哪个节点
	// 多试几个 key 避免恰好哈希到 self
	var getter PeerGetter
	var ok bool
	testKey := ""
	for _, k := range []string{"multi-key", "key-a", "key-b", "key-c", "key-d"} {
		if g, found := pools[0].PickPeer(k); found && g != nil {
			getter = g
			ok = true
			testKey = k
			break
		}
	}
	if !ok {
		t.Fatal("expected to pick a peer, but all keys hashed to self")
	}

	req := &pb.Request{Group: "shared", Key: testKey}
	resp := &pb.Response{}
	if err := getter.Get(req, resp); err != nil {
		t.Fatalf("multi-node Get failed: %v", err)
	}
	expected := "shared-" + testKey
	if string(resp.Value) != expected {
		t.Fatalf("expected %q, got %q", expected, resp.Value)
	}
}
