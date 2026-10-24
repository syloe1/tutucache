package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"geecache"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

const (
	peersFile = "peers.json"
	apiURL    = "http://localhost:9999"

	// HTTP 服务端超时：防止 slowloris / 慢连接无限期占用 goroutine 和 socket。
	// WriteTimeout 需明显大于 peer 客户端的 2s 请求超时（见 http.go），
	// 给远程回源和较慢的本地 getter 留出余量。
	serverReadHeaderTimeout = 5 * time.Second
	serverReadTimeout       = 10 * time.Second
	serverWriteTimeout      = 10 * time.Second
	serverIdleTimeout       = 60 * time.Second
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func createGroup() *geecache.Group {
	// 最大内存两KB
	return geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

// tlsSettings 是从环境变量解析出的节点间 TLS 配置。
type tlsSettings struct {
	enabled  bool
	certFile string
	keyFile  string
	caFile   string
}

// tlsSettingsFromEnv 解析并校验 GEECACHE_CERT / GEECACHE_KEY / GEECACHE_CA。
// cert 与 key 必须成对出现；只设置 CA 不会开启 TLS，属于配置错误。
func tlsSettingsFromEnv() (tlsSettings, error) {
	certFile := os.Getenv("GEECACHE_CERT")
	keyFile := os.Getenv("GEECACHE_KEY")
	caFile := os.Getenv("GEECACHE_CA")

	if (certFile == "") != (keyFile == "") {
		return tlsSettings{}, fmt.Errorf("GEECACHE_CERT and GEECACHE_KEY must be set together")
	}
	if certFile == "" && caFile != "" {
		return tlsSettings{}, fmt.Errorf("GEECACHE_CA requires GEECACHE_CERT and GEECACHE_KEY")
	}
	return tlsSettings{
		enabled:  certFile != "",
		certFile: certFile,
		keyFile:  keyFile,
		caFile:   caFile,
	}, nil
}

// newHTTPServer 构造带统一超时配置的 HTTP 服务端。
// 节点服务与 API 服务共用，避免其中一个入口漏配超时而重新引入 slowloris 面。
func newHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		WriteTimeout:      serverWriteTimeout,
		IdleTimeout:       serverIdleTimeout,
	}
}

// serve 阻塞运行 HTTP 服务，正常关闭（ErrServerClosed）时返回 nil，其余错误原样返回。
// 单独抽出便于单元测试；调用方负责把非 nil 错误交给 main 处理。
func serve(srv *http.Server, useTLS bool, certFile, keyFile string) error {
	var err error
	if useTLS {
		err = srv.ListenAndServeTLS(certFile, keyFile)
	} else {
		err = srv.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// listenAddrFromURL 从节点对外 URL 中提取监听用的 host:port。
// 监听地址与 scheme 无关：http:// 和 https:// 绑定的是同一个 host:port，
// 因此不能按 "http://" 的固定长度截取。scheme 只影响 peer 拨号时使用的 URL。
func listenAddrFromURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("parse %q: missing host (want scheme://host:port)", rawURL)
	}
	if _, _, err := net.SplitHostPort(u.Host); err != nil {
		return "", fmt.Errorf("parse %q: %w", rawURL, err)
	}
	return u.Host, nil
}

// startCacheServer 启动**集群内部节点服务**（节点之间通信）。
// selfURL 是节点对外公布的地址（peer 用它拨号）；listenAddr 是本机监听的 host:port。
// 同步阶段的错误（TLS、服务发现）通过返回值交给 main，不在这里 os.Exit；
// 服务运行期间的错误通过 serveErr 上报，由 main 触发统一的关闭流程。
func startCacheServer(selfURL, listenAddr string, gee *geecache.Group, tlsCfg tlsSettings, serveErr chan<- error) (*http.Server, *geecache.FileDiscovery, error) {
	pool := geecache.NewHTTPPool(selfURL)

	// 共享 Token 认证（环境变量 GEECACHE_TOKEN）
	if token := os.Getenv("GEECACHE_TOKEN"); token != "" {
		pool.SetSharedToken(token)
		log.Println("shared token auth enabled")
	}

	if tlsCfg.enabled {
		// TLS 配置失败属于启动错误，返回给 main 决定退出。
		if err := pool.EnableTLS(tlsCfg.certFile, tlsCfg.keyFile, tlsCfg.caFile); err != nil {
			return nil, nil, fmt.Errorf("enable TLS: %w", err)
		}
		if tlsCfg.caFile != "" {
			log.Println("TLS enabled with mTLS (client certificates required and verified)")
		} else {
			log.Println("TLS enabled")
		}
	}

	// 校验 selfURL 的 scheme 与 TLS 模式一致：TLS 开启时必须注册 https://，
	// 否则对端会按 http:// 拨号，握手必然失败。
	u, err := url.Parse(selfURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse self URL %q: %w", selfURL, err)
	}
	if u.Host == "" {
		return nil, nil, fmt.Errorf("parse self URL %q: missing host (want scheme://host:port)", selfURL)
	}
	wantScheme := "http"
	if tlsCfg.enabled {
		wantScheme = "https"
	}
	if !strings.EqualFold(u.Scheme, wantScheme) {
		return nil, nil, fmt.Errorf("tls enabled=%t but self URL scheme is %q (want %q)", tlsCfg.enabled, u.Scheme, wantScheme)
	}

	// 先同步绑定端口，避免注册成功后才发现端口不可用。
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	// 端口绑定成功后再注册，确保 peers.json 中不会留下起不来的节点。
	discovery := geecache.NewFileDiscovery(selfURL, peersFile)
	// 给 HTTPPool 绑定服务发现。
	if err := pool.StartDiscovery(discovery); err != nil {
		_ = ln.Close()
		return nil, nil, fmt.Errorf("register with discovery: %w", err)
	}
	// 给Group注册 [节点选择器PeerPicker]
	gee.RegisterPeers(pool)

	srv := newHTTPServer(pool)
	// TLS 未启用时该值为 nil，Serve 会直接处理明文连接。
	srv.TLSConfig = pool.TLSConfig()
	// 在后台异步启动缓存节点的 HTTP 服务。
	go func() {
		log.Printf("geecache is running at %s", listenAddr)
		var err error
		if tlsCfg.enabled {
			err = srv.ServeTLS(ln, tlsCfg.certFile, tlsCfg.keyFile)
		} else {
			err = srv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			// 运行错误交给 main，由它触发与信号相同的关闭流程。
			serveErr <- fmt.Errorf("geecache server: %w", err)
			return
		}
		log.Printf("geecache server %s stopped", listenAddr)
	}()

	return srv, discovery, nil
}

// startAPIServer 启动前端 API 服务；listenAddr 是本机监听的 host:port。
// 使用独立 mux，避免污染全局 DefaultServeMux，也便于测试中重复创建。
func startAPIServer(listenAddr string, gee *geecache.Group, serveErr chan<- error) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			view, err := gee.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(view.ByteSlice())
		}))

	srv := newHTTPServer(mux)
	srv.Addr = listenAddr

	//新开 goroutine 异步启动 API 服务：
	go func() {
		log.Printf("frontend server is running at %s", listenAddr)
		//`ListenAndServe()` 阻塞，监听 9999 端口，接收用户 http 请求
		if err := serve(srv, false, "", ""); err != nil {
			serveErr <- fmt.Errorf("API server: %w", err)
			return
		}
		log.Printf("frontend server %s stopped", listenAddr)
	}()

	return srv
}

// stopServers 在超时时间内关闭所有已启动的 HTTP 服务；nil 会被跳过。
// 前端 API、admin 与缓存节点共用一个关闭超时，避免串行等待导致总时长失控。
func stopServers(servers ...*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, srv := range servers {
		if srv == nil {
			continue
		}
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("HTTP server shutdown warning: %v", err)
		}
	}
}

// parseTransport 校验 -transport 的取值。
func parseTransport(v string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "http":
		return "http", nil
	case "grpc":
		return "grpc", nil
	default:
		return "", fmt.Errorf("unsupported transport %q (use http or grpc)", v)
	}
}

// startGRPCCacheServer 启动**集群内部 gRPC 节点服务**。
// selfAddr 是对外公布的裸 host:port（gRPC target 不接受 URL scheme），
// listenAddr 是本机监听的 host:port，测试中可以是 127.0.0.1:0。
func startGRPCCacheServer(selfAddr, listenAddr string, gee *geecache.Group, tlsCfg tlsSettings, serveErr chan<- error) (*geecache.GRPCPool, *geecache.FileDiscovery, error) {
	if _, _, err := net.SplitHostPort(selfAddr); err != nil {
		return nil, nil, fmt.Errorf("invalid gRPC self address %q (want host:port): %w", selfAddr, err)
	}

	pool := geecache.NewGRPCPool(selfAddr)

	if token := os.Getenv("GEECACHE_TOKEN"); token != "" {
		pool.SetSharedToken(token)
		log.Println("shared token auth enabled")
	}

	if tlsCfg.enabled {
		if err := pool.EnableTLS(tlsCfg.certFile, tlsCfg.keyFile, tlsCfg.caFile); err != nil {
			return nil, nil, fmt.Errorf("enable TLS: %w", err)
		}
		if tlsCfg.caFile != "" {
			log.Println("TLS enabled with mTLS (client certificates required and verified)")
		} else {
			log.Println("TLS enabled")
		}
	}

	// 先同步绑定端口，避免注册成功后才发现端口不可用。
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}

	// gRPC 的节点地址是裸 host:port，不能与 HTTP 节点混用 peers.json。
	discovery := geecache.NewFileDiscovery(selfAddr, peersFile)
	if err := pool.StartDiscovery(discovery); err != nil {
		_ = ln.Close()
		return nil, nil, fmt.Errorf("register with discovery: %w", err)
	}
	gee.RegisterPeers(pool)

	go func() {
		log.Printf("geecache gRPC server is running at %s", listenAddr)
		if err := pool.ServeListener(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- fmt.Errorf("geecache gRPC server: %w", err)
			return
		}
		log.Printf("geecache gRPC server %s stopped", listenAddr)
	}()

	return pool, discovery, nil
}

// startAdminServer 在独立端口上提供 /health 与 /metrics。
// gRPC 模式没有 HTTP 节点 handler，健康检查和指标需要这个入口。
func startAdminServer(listenAddr string, serveErr chan<- error) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/health", geecache.HealthHandler())
	mux.Handle("/metrics", geecache.MetricsHandler())

	srv := newHTTPServer(mux)
	// 同步绑定，端口冲突作为启动错误返回。
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	srv.Addr = ln.Addr().String()

	go func() {
		log.Printf("admin server is running at %s", ln.Addr())
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("admin server: %w", err)
			return
		}
		log.Printf("admin server stopped")
	}()

	return srv, nil
}

// run 封装所有启动、等待、关闭逻辑，返回值为进程退出时的错误（nil = 正常信号退出）。
// main 仅负责根据 run 的返回值决定退出码，所有 defer 都能正常执行，不会被 os.Exit 跳过。
func run() error {
	var port int
	var api bool
	var transport string
	var adminAddr string
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.StringVar(&transport, "transport", "http", "peer transport: http or grpc")
	flag.StringVar(&adminAddr, "admin-addr", "", "optional admin HTTP listen address for /health and /metrics (e.g. 127.0.0.1:9001)")
	flag.Parse()

	transport, err := parseTransport(transport)
	if err != nil {
		return err
	}
	/*

	   # port=8001，api=false（不启动9999前端API）
	   go run main.go -port 8001

	   # port=8001，api=true（启动9999前端API服务）
	   go run main.go -port 8001 -api

	*/
	// 每个端口对应一个地址
	// 端口 -> 节点完整地址映射
	portToHost := map[int]string{
		8001: "localhost:8001",
		8002: "localhost:8002",
		8003: "localhost:8003",
	}
	hostAddr, ok := portToHost[port]
	if !ok {
		return fmt.Errorf("unsupported port: %d (use 8001/8002/8003)", port)
	}

	// 解析并校验 TLS 环境变量。HTTP 模式还需要用它决定注册 URL 的 scheme；
	// gRPC 的 target 是裸 host:port，不携带 scheme。
	tlsCfg, err := tlsSettingsFromEnv()
	if err != nil {
		return fmt.Errorf("invalid TLS configuration: %w", err)
	}

	var selfURL, listenAddr, registeredAddr string
	if transport == "grpc" {
		// 服务发现中登记裸地址；监听同一地址。
		registeredAddr = hostAddr
		listenAddr = hostAddr
	} else {
		scheme := "http"
		if tlsCfg.enabled {
			scheme = "https"
		}
		selfURL = fmt.Sprintf("%s://%s", scheme, hostAddr)
		// 监听地址从对外 URL 中解析出来：与 scheme 无关，http/https 都监听同一个 host:port。
		listenAddr, err = listenAddrFromURL(selfURL)
		if err != nil {
			return fmt.Errorf("invalid cache node address: %w", err)
		}
		registeredAddr = selfURL
	}

	//创建缓存 Group 实例
	gee := createGroup()

	// 服务运行错误的上报通道；容量等于可能上报错误的 server 数量（cache + api + admin）。
	serveErr := make(chan error, 3)

	// 启动服务
	var apiSrv *http.Server
	if api {
		apiListenAddr, err := listenAddrFromURL(apiURL)
		if err != nil {
			return fmt.Errorf("invalid API address: %w", err)
		}
		apiSrv = startAPIServer(apiListenAddr, gee, serveErr)
	}

	var adminSrv *http.Server
	if adminAddr != "" {
		adminSrv, err = startAdminServer(adminAddr, serveErr)
		if err != nil {
			// API 服务可能已经启动，先关闭再退出。
			stopServers(apiSrv)
			gee.Close()
			return fmt.Errorf("start admin server: %w", err)
		}
	}

	//启动**集群内部通信缓存节点服务**
	var (
		cacheSrv  *http.Server
		grpcPool  *geecache.GRPCPool
		discovery *geecache.FileDiscovery
	)
	if transport == "grpc" {
		grpcPool, discovery, err = startGRPCCacheServer(registeredAddr, listenAddr, gee, tlsCfg, serveErr)
	} else {
		cacheSrv, discovery, err = startCacheServer(selfURL, listenAddr, gee, tlsCfg, serveErr)
	}
	if err != nil {
		// 其他服务可能已经启动，先关闭再退出，避免端口和 goroutine 泄漏。
		stopServers(apiSrv, adminSrv)
		gee.Close()
		return fmt.Errorf("start cache server: %w", err)
	}

	// =========================================================================
	// 等待退出信号或服务运行时错误，执行优雅关闭
	// =========================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	var shutdownCause error
	select {
	case sig := <-quit:
		log.Printf("received signal %s, starting graceful shutdown...", sig)
	case shutdownCause = <-serveErr:
		log.Printf("server runtime error: %v, starting graceful shutdown...", shutdownCause)
	}
	// 先停止 Watch/心跳，再注销；否则心跳会把刚删除的节点重新写回。
	discovery.Close()
	if err := discovery.Unregister(registeredAddr); err != nil {
		log.Printf("[WARN] failed to unregister %s: %v", registeredAddr, err)
	}
	stopServers(apiSrv, adminSrv, cacheSrv)
	if grpcPool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := grpcPool.Shutdown(ctx); err != nil {
			log.Printf("gRPC server shutdown warning: %v", err)
		}
		cancel()
	}
	// 停止 Group 的后台清理循环（若通过 SetTTL 启动过）。
	gee.Close()

	// 日志措辞区分正常/错误触发的关闭，避免误导
	if shutdownCause != nil {
		log.Println("shutdown complete (triggered by runtime error)")
	} else {
		log.Println("shutdown complete (graceful)")
	}
	return shutdownCause
}

func main() {
	if err := run(); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}
