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
	"syscall"
	"time"
)

const (
	peersFile = "peers.json"
	apiURL    = "http://localhost:9999"
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
func startCacheServer(selfURL, listenAddr string, gee *geecache.Group, serveErr chan<- error) (*http.Server, error) {
	pool := geecache.NewHTTPPool(selfURL)

	// 共享 Token 认证（环境变量 GEECACHE_TOKEN）
	if token := os.Getenv("GEECACHE_TOKEN"); token != "" {
		pool.SetSharedToken(token)
		log.Println("shared token auth enabled")
	}

	// TLS 加密（环境变量 GEECACHE_CERT / GEECACHE_KEY / GEECACHE_CA）
	certFile := os.Getenv("GEECACHE_CERT")
	keyFile := os.Getenv("GEECACHE_KEY")
	if certFile != "" && keyFile != "" {
		caFile := os.Getenv("GEECACHE_CA")
		// TLS 配置失败属于启动错误，返回给 main 决定退出。
		if err := pool.EnableTLS(certFile, keyFile, caFile); err != nil {
			return nil, fmt.Errorf("TLS setup: %w", err)
		}
		log.Println("TLS enabled")
	}

	// 服务发现
	discovery := geecache.NewFileDiscovery(selfURL, peersFile)
	// 给HTTPPool绑定服务发现
	if err := pool.StartDiscovery(discovery); err != nil {
		return nil, fmt.Errorf("discovery register: %w", err)
	}
	// 给Group注册 [节点选择器PeerPicker]
	gee.RegisterPeers(pool)

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: pool,
	}
	//在后台异步启动缓存节点的 HTTP 服务
	go func() {
		log.Printf("geecache is running at %s", listenAddr)
		useTLS := pool.TLSConfig() != nil
		if err := serve(srv, useTLS, certFile, keyFile); err != nil {
			// 运行错误交给 main，由它触发与信号相同的关闭流程。
			serveErr <- fmt.Errorf("geecache server: %w", err)
			return
		}
		log.Printf("geecache server %s stopped", listenAddr)
	}()

	return srv, nil
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

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

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

// stopServers 在超时时间内关闭所有已启动的 HTTP 服务；nil 表示该服务未启动。
func stopServers(apiSrv, cacheSrv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if apiSrv != nil {
		if err := apiSrv.Shutdown(ctx); err != nil {
			log.Printf("API server shutdown warning: %v", err)
		}
	}
	if cacheSrv != nil {
		if err := cacheSrv.Shutdown(ctx); err != nil {
			log.Printf("cache server shutdown warning: %v", err)
		}
	}
}

// run 封装所有启动、等待、关闭逻辑，返回值为进程退出时的错误（nil = 正常信号退出）。
// main 仅负责根据 run 的返回值决定退出码，所有 defer 都能正常执行，不会被 os.Exit 跳过。
func run() error {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()
	/*

	   # port=8001，api=false（不启动9999前端API）
	   go run main.go -port 8001

	   # port=8001，api=true（启动9999前端API服务）
	   go run main.go -port 8001 -api

	*/
	// 每个端口对应一个地址
	// 端口 -> 节点完整地址映射
	portToAddr := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}
	selfURL := portToAddr[port]
	if selfURL == "" {
		return fmt.Errorf("unsupported port: %d (use 8001/8002/8003)", port)
	}
	// 监听地址从对外 URL 中解析出来：与 scheme 无关，http/https 都监听同一个 host:port。
	listenAddr, err := listenAddrFromURL(selfURL)
	if err != nil {
		return fmt.Errorf("invalid cache node address: %w", err)
	}

	//创建缓存 Group 实例
	gee := createGroup()

	// 服务运行错误的上报通道；容量等于可能上报错误的 server 数量（cache + api）。
	serveErr := make(chan error, 2)

	// 启动服务
	var apiSrv *http.Server
	if api {
		apiListenAddr, err := listenAddrFromURL(apiURL)
		if err != nil {
			return fmt.Errorf("invalid API address: %w", err)
		}
		apiSrv = startAPIServer(apiListenAddr, gee, serveErr)
	}
	//启动**集群内部通信缓存节点服务**
	cacheSrv, err := startCacheServer(selfURL, listenAddr, gee, serveErr)
	if err != nil {
		// API 服务可能已经启动，先关闭再退出，避免端口和 goroutine 泄漏。
		stopServers(apiSrv, nil)
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

	stopServers(apiSrv, cacheSrv)
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
