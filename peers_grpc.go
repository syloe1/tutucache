package geecache

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"geecache/consistenthash"
	pb "geecache/geecachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultGRPCReplicas = 50
)

// =============================================================================
// gRPC 服务端 —— 实现 protoc 生成的 GroupCacheServer 接口
// =============================================================================

// GRPCServer 实现了 geecachepb.GroupCacheServer，处理来自其他节点的 gRPC 请求
type GRPCServer struct {
	pb.UnimplementedGroupCacheServer
}

// Get 是 gRPC 服务端的方法：接收远程节点的缓存查询，走本地 Group 的完整查询链路
func (s *GRPCServer) Get(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	group := GetGroup(req.GetGroup())
	if group == nil {
		return nil, status.Errorf(codes.NotFound, "no such group: %s", req.GetGroup())
	}
	view, err := group.Get(req.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.Response{Value: view.ByteSlice()}, nil
}

// =============================================================================
// gRPC 客户端 —— 实现项目的 PeerGetter 接口
// =============================================================================

// grpcGetter 封装了 protoc 生成的 gRPC 客户端，并适配为 PeerGetter 接口
type grpcGetter struct {
	addr   string
	client pb.GroupCacheClient
}

// 编译期断言：grpcGetter 实现了 PeerGetter
var _ PeerGetter = (*grpcGetter)(nil)

// Get 适配层：gRPC 风格 (ctx, req) → (*Resp, error)  →  PeerGetter 风格 (in, out) → error
func (g *grpcGetter) Get(in *pb.Request, out *pb.Response) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := g.client.Get(ctx, in)
	if err != nil {
		return err
	}
	// 用 proto.Merge 而非 *out = *resp，避免拷贝 protobuf 内部的 sync.Mutex
	proto.Merge(out, resp)
	return nil
}

// =============================================================================
// gRPC 节点池 —— 实现项目的 PeerPicker 接口
// =============================================================================
// GRPCPool 管理一组 gRPC 缓存节点（类似 HTTPPool），负责节点发现和负载均衡
type GRPCPool struct {
	self        string                 // 本节点地址，如 "localhost:8001"
	mu          sync.Mutex             // 保护 peers 和 grpcGetters
	peers       *consistenthash.Map    // 一致性哈希环，映射 key → 节点
	grpcGetters map[string]*grpcGetter // 节点地址 → gRPC 客户端
	grpcServer  *grpc.Server           // 本节点的 gRPC server
}

// 编译期断言：GRPCPool 实现了 PeerPicker
var _ PeerPicker = (*GRPCPool)(nil)

// NewGRPCPool 创建一个 gRPC 节点池，self 为本节点监听地址
func NewGRPCPool(self string) *GRPCPool {
	return &GRPCPool{
		self:        self,
		grpcGetters: make(map[string]*grpcGetter),
		grpcServer:  grpc.NewServer(),
	}
}

// Log 统一日志前缀
func (p *GRPCPool) Log(format string, v ...interface{}) {
	log.Printf("[GRPCPool %s] %s", p.self, fmt.Sprintf(format, v...))
}

// Set 初始化集群中的所有节点，每个节点复用已建立的 gRPC 连接
func (p *GRPCPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 构建一致性哈希环
	p.peers = consistenthash.New(defaultGRPCReplicas, nil)
	p.peers.Add(peers...)

	// 为每个对端节点创建 gRPC 客户端（复用连接）
	for _, peer := range peers {
		if peer == p.self {
			continue // 不需要给自己创建客户端
		}
		if _, ok := p.grpcGetters[peer]; ok {
			continue // 已存在，跳过
		}
		conn, err := grpc.NewClient(
			peer,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			p.Log("failed to dial %s: %v", peer, err)
			continue
		}
		p.grpcGetters[peer] = &grpcGetter{
			addr:   peer,
			client: pb.NewGroupCacheClient(conn),
		}
	}
}

// PickPeer 根据 key 选择应访问的远程节点
func (p *GRPCPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.peers == nil {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s for key %s", peer, key)
		if getter, ok := p.grpcGetters[peer]; ok {
			return getter, true
		}
	}
	return nil, false
}

// Serve 启动 gRPC 服务，注册本节点为 GroupCacheServer，阻塞直到出错
func (p *GRPCPool) Serve() error {
	// 将本节点注册为 gRPC GroupCacheServer
	pb.RegisterGroupCacheServer(p.grpcServer, &GRPCServer{})

	lis, err := net.Listen("tcp", p.self)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.self, err)
	}

	p.Log("gRPC server listening on %s", p.self)
	return p.grpcServer.Serve(lis)
}

// Stop 优雅关闭 gRPC 服务
func (p *GRPCPool) Stop() {
	p.grpcServer.GracefulStop()
}
