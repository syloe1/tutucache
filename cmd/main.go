package main

import (
	"context"
	"flag"
	"fmt"
	"geecache"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func createGroup() *geecache.Group {
	return geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(addr string, addrs []string, gee *geecache.Group) *http.Server {
	peers := geecache.NewHTTPPool(addr)
	peers.Set(addrs...)
	gee.RegisterPeers(peers)

	hostPort := addr[7:] // 去掉 "http://" 前缀
	srv := &http.Server{
		Addr:    hostPort,
		Handler: peers,
	}

	go func() {
		log.Printf("geecache is running at %s", hostPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("geecache server error: %v", err)
		}
		log.Printf("geecache server %s stopped", hostPort)
	}()

	return srv
}

func startAPIServer(apiAddr string, gee *geecache.Group) *http.Server {
	http.Handle("/api", http.HandlerFunc(
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

	hostPort := apiAddr[7:]
	srv := &http.Server{
		Addr:    hostPort,
		Handler: nil, // 使用 DefaultServeMux
	}

	go func() {
		log.Printf("frontend server is running at %s", hostPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("API server error: %v", err)
		}
		log.Printf("frontend server %s stopped", hostPort)
	}()

	return srv
}

func main() {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	apiAddr := "http://localhost:9999"
	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	gee := createGroup()

	// 启动服务
	var cacheSrv, apiSrv *http.Server
	if api {
		apiSrv = startAPIServer(apiAddr, gee)
	}
	cacheSrv = startCacheServer(addrMap[port], addrs, gee)

	// =========================================================================
	// 等待退出信号，执行优雅关闭
	// =========================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("received signal %s, shutting down...", sig)

	// 给每个 server 最多 10 秒完成正在处理的请求
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if apiSrv != nil {
		if err := apiSrv.Shutdown(ctx); err != nil {
			log.Printf("API server shutdown error: %v", err)
		}
	}
	if err := cacheSrv.Shutdown(ctx); err != nil {
		log.Printf("cache server shutdown error: %v", err)
	}

	log.Println("all servers stopped gracefully")
}
