package main

import (
	"context"
	"geecache"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestListenAddrFromURL(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		want    string
		wantErr bool
	}{
		{name: "http", rawURL: "http://localhost:8001", want: "localhost:8001"},
		{name: "https", rawURL: "https://localhost:8001", want: "localhost:8001"},
		{name: "ipv4", rawURL: "http://127.0.0.1:8001", want: "127.0.0.1:8001"},
		{name: "ipv6", rawURL: "http://[::1]:8001", want: "[::1]:8001"},
		{name: "path is ignored", rawURL: "http://localhost:8001/geecache/", want: "localhost:8001"},
		{name: "missing scheme", rawURL: "localhost:8001", wantErr: true},
		{name: "missing port", rawURL: "https://localhost", wantErr: true},
		{name: "missing host", rawURL: "http://", wantErr: true},
	}

	for _, tt := range tests {
		got, err := listenAddrFromURL(tt.rawURL)
		if tt.wantErr {
			if err == nil {
				t.Errorf("%s: listenAddrFromURL(%q) = %q, want error", tt.name, tt.rawURL, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: listenAddrFromURL(%q) error: %v", tt.name, tt.rawURL, err)
			continue
		}
		if got != tt.want {
			t.Errorf("%s: listenAddrFromURL(%q) = %q, want %q", tt.name, tt.rawURL, got, tt.want)
		}
	}
}

// TestServeBindConflict 端口被占用时 serve 返回错误
func TestServeBindConflict(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy test port: %v", err)
	}
	defer ln.Close()

	srv := &http.Server{
		Addr:    ln.Addr().String(),
		Handler: http.NewServeMux(),
	}
	err = serve(srv, false, "", "")
	if err == nil {
		t.Fatal("expected bind conflict error, got nil")
	}
}

// TestServeGracefulShutdown 调用 Shutdown 后 serve 返回 nil（http.ErrServerClosed）
func TestServeGracefulShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get test port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := &http.Server{
		Addr:    addr,
		Handler: http.NewServeMux(),
	}

	done := make(chan error, 1)
	go func() {
		done <- serve(srv, false, "", "")
	}()

	// 等待监听成功，带超时
	ready := make(chan struct{}, 1)
	go func() {
		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) {
			conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				ready <- struct{}{}
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	select {
	case <-ready:
	case <-time.After(1 * time.Second):
		t.Fatal("server did not become ready within 1s")
	}

	// 触发优雅关闭
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error after graceful shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return after shutdown, timeout")
	}
}

// startCacheServer 的同步启动错误（如 TLS 证书加载失败）必须返回给调用方，
// 而不是在内部 os.Exit，否则已经启动的 API 服务会被直接带走。
func TestStartCacheServerTLSFailureReturnsError(t *testing.T) {
	// 注入不存在的证书文件，触发 TLS 初始化失败
	t.Setenv("GEECACHE_CERT", "/nonexistent/cert.pem")
	t.Setenv("GEECACHE_KEY", "/nonexistent/key.pem")

	gee := geecache.NewGroup("startup-tls-failure", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	_, _, err := startCacheServer("http://localhost:8001", "127.0.0.1:0", gee, make(chan error, 2))
	if err == nil {
		t.Fatal("expected error when TLS cert cannot be loaded, got nil")
	}
}

// 服务运行时的错误必须上报到 serveErr 通道，由 main 触发统一的关闭流程。
func TestStartAPIServerReportsServeError(t *testing.T) {
	// 先占用端口制造 bind 冲突
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy port: %v", err)
	}
	defer ln.Close()

	gee := geecache.NewGroup("api-serve-error", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	serveErr := make(chan error, 1)
	srv := startAPIServer(ln.Addr().String(), gee, serveErr)
	defer srv.Close()

	select {
	case err := <-serveErr:
		if err == nil {
			t.Fatal("expected non-nil bind error on serveErr channel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve error was not reported on channel within timeout")
	}
}
