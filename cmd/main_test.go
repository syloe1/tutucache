package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"geecache"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
	tlsCfg := tlsSettings{
		enabled:  true,
		certFile: "/nonexistent/cert.pem",
		keyFile:  "/nonexistent/key.pem",
	}

	gee := geecache.NewGroup("startup-tls-failure", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	_, _, err := startCacheServer("https://localhost:8001", "127.0.0.1:0", gee, tlsCfg, make(chan error, 2))
	if err == nil {
		t.Fatal("expected error when TLS cert cannot be loaded, got nil")
	}
}

func TestTLSSettingsFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		cert    string
		key     string
		ca      string
		want    tlsSettings
		wantErr bool
	}{
		{name: "disabled", want: tlsSettings{}},
		{name: "tls", cert: "cert.pem", key: "key.pem", want: tlsSettings{
			enabled: true, certFile: "cert.pem", keyFile: "key.pem",
		}},
		{name: "mtls", cert: "cert.pem", key: "key.pem", ca: "ca.pem", want: tlsSettings{
			enabled: true, certFile: "cert.pem", keyFile: "key.pem", caFile: "ca.pem",
		}},
		{name: "cert without key", cert: "cert.pem", wantErr: true},
		{name: "key without cert", key: "key.pem", wantErr: true},
		{name: "ca without cert key", ca: "ca.pem", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GEECACHE_CERT", tt.cert)
			t.Setenv("GEECACHE_KEY", tt.key)
			t.Setenv("GEECACHE_CA", tt.ca)

			got, err := tlsSettingsFromEnv()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("tlsSettingsFromEnv() = %+v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("tlsSettingsFromEnv() error: %v", err)
			}
			if got != tt.want {
				t.Errorf("tlsSettingsFromEnv() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TLS 模式与 self URL 的 scheme 必须一致，否则对端会按错误协议拨号。
func TestStartCacheServerRejectsSchemeMismatch(t *testing.T) {
	gee := geecache.NewGroup("startup-scheme-mismatch", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	_, _, err := startCacheServer("https://localhost:8001", "127.0.0.1:0", gee, tlsSettings{}, make(chan error, 2))
	if err == nil {
		t.Fatal("expected error when self URL scheme does not match TLS mode, got nil")
	}
}

// startCacheServer 必须把 EnableTLS 构建的服务端配置挂到 http.Server 上，
// 否则 ClientAuth/ClientCAs 等设置会被 ServeTLS 静默丢弃。
func TestStartCacheServerAppliesTLSConfig(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t, t.TempDir())
	// peers.json 是相对路径，切到临时目录避免污染仓库。
	t.Chdir(t.TempDir())

	gee := geecache.NewGroup("startup-tls-config", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))
	tlsCfg := tlsSettings{enabled: true, certFile: certFile, keyFile: keyFile}

	srv, discovery, err := startCacheServer("https://localhost:8001", "127.0.0.1:0", gee, tlsCfg, make(chan error, 2))
	if err != nil {
		t.Fatalf("startCacheServer() error: %v", err)
	}
	defer discovery.Close()
	defer srv.Close()

	if srv.TLSConfig == nil {
		t.Fatal("server TLS config was not applied")
	}
	if srv.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want %x", srv.TLSConfig.MinVersion, tls.VersionTLS12)
	}
	if srv.TLSConfig.ClientAuth != tls.NoClientCert {
		t.Errorf("ClientAuth = %v, want NoClientCert when no CA is configured", srv.TLSConfig.ClientAuth)
	}
}

// 回归测试 #19：两个 HTTP 服务端都必须配置读/写/空闲超时，
// 否则慢速连接可以无限期占用 goroutine 和 socket（slowloris）。
func TestStartCacheServerSetsTimeouts(t *testing.T) {
	// peers.json 是相对路径，切到临时目录避免污染仓库。
	t.Chdir(t.TempDir())

	gee := geecache.NewGroup("startup-timeouts", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	srv, discovery, err := startCacheServer("http://localhost:8001", "127.0.0.1:0", gee, tlsSettings{}, make(chan error, 2))
	if err != nil {
		t.Fatalf("startCacheServer() error: %v", err)
	}
	defer discovery.Close()
	defer srv.Close()

	assertServerTimeouts(t, srv)
}

func TestStartAPIServerSetsTimeouts(t *testing.T) {
	gee := geecache.NewGroup("api-timeouts", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	srv := startAPIServer("127.0.0.1:0", gee, make(chan error, 1))
	defer srv.Close()

	assertServerTimeouts(t, srv)
}

func assertServerTimeouts(t *testing.T, srv *http.Server) {
	t.Helper()
	if srv.ReadHeaderTimeout != serverReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, serverReadHeaderTimeout)
	}
	if srv.ReadTimeout != serverReadTimeout {
		t.Errorf("ReadTimeout = %v, want %v", srv.ReadTimeout, serverReadTimeout)
	}
	if srv.WriteTimeout != serverWriteTimeout {
		t.Errorf("WriteTimeout = %v, want %v", srv.WriteTimeout, serverWriteTimeout)
	}
	if srv.IdleTimeout != serverIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", srv.IdleTimeout, serverIdleTimeout)
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

func TestParseTransport(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "http", want: "http"},
		{in: "HTTP", want: "http"},
		{in: " grpc ", want: "grpc"},
		{in: "GRPC", want: "grpc"},
		{in: "", wantErr: true},
		{in: "tcp", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseTransport(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseTransport(%q) = %q, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTransport(%q) error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("parseTransport(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// gRPC 节点服务必须注册裸 host:port，并能在 Shutdown 后干净退出。
func TestStartGRPCCacheServer(t *testing.T) {
	t.Chdir(t.TempDir())

	gee := geecache.NewGroup("startup-grpc", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))
	serveErr := make(chan error, 1)

	const selfAddr = "127.0.0.1:18001"
	pool, discovery, err := startGRPCCacheServer(selfAddr, "127.0.0.1:0", gee, tlsSettings{}, serveErr)
	if err != nil {
		t.Fatalf("startGRPCCacheServer() error: %v", err)
	}
	defer discovery.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pool.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error: %v", err)
		}
	}()

	peers, err := discovery.GetPeers()
	if err != nil {
		t.Fatalf("GetPeers() error: %v", err)
	}
	if !slices.Contains(peers, selfAddr) {
		t.Errorf("peers.json = %v, want it to contain %s", peers, selfAddr)
	}

	select {
	case err := <-serveErr:
		t.Fatalf("unexpected runtime error: %v", err)
	default:
	}
}

func TestStartGRPCCacheServerBindFailure(t *testing.T) {
	t.Chdir(t.TempDir())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy port: %v", err)
	}
	defer ln.Close()

	gee := geecache.NewGroup("startup-grpc-bind", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	_, _, err = startGRPCCacheServer("127.0.0.1:18001", ln.Addr().String(), gee, tlsSettings{}, make(chan error, 1))
	if err == nil {
		t.Fatal("expected bind conflict error, got nil")
	}
}

func TestStartGRPCCacheServerTLSFailure(t *testing.T) {
	t.Chdir(t.TempDir())

	gee := geecache.NewGroup("startup-grpc-tls", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))
	tlsCfg := tlsSettings{
		enabled:  true,
		certFile: "/nonexistent/cert.pem",
		keyFile:  "/nonexistent/key.pem",
	}

	_, _, err := startGRPCCacheServer("127.0.0.1:18001", "127.0.0.1:0", gee, tlsCfg, make(chan error, 1))
	if err == nil {
		t.Fatal("expected error when TLS cert cannot be loaded, got nil")
	}
}

// gRPC target 不接受 URL scheme，配置错误必须在启动阶段暴露。
func TestStartGRPCCacheServerRejectsURLSelf(t *testing.T) {
	t.Chdir(t.TempDir())

	gee := geecache.NewGroup("startup-grpc-url", 1<<10, geecache.GetterFunc(func(string) ([]byte, error) {
		return nil, nil
	}))

	_, _, err := startGRPCCacheServer("http://127.0.0.1:18001", "127.0.0.1:0", gee, tlsSettings{}, make(chan error, 1))
	if err == nil {
		t.Fatal("expected error for URL-form gRPC self address, got nil")
	}
}

// admin 端口暴露 /health 与 /metrics，并且沿用统一的超时配置。
func TestStartAdminServer(t *testing.T) {
	serveErr := make(chan error, 1)
	srv, err := startAdminServer("127.0.0.1:0", serveErr)
	if err != nil {
		t.Fatalf("startAdminServer() error: %v", err)
	}
	defer srv.Close()

	assertServerTimeouts(t, srv)

	get := func(path string) (int, string) {
		t.Helper()
		resp, err := http.Get("http://" + srv.Addr + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read GET %s: %v", path, err)
		}
		return resp.StatusCode, string(body)
	}

	if code, body := get("/health"); code != http.StatusOK || body != "OK" {
		t.Errorf("GET /health = %d %q, want 200 OK", code, body)
	}
	code, body := get("/metrics")
	if code != http.StatusOK {
		t.Errorf("GET /metrics status = %d, want 200", code)
	}
	if !strings.Contains(body, "geecache_hits_total") {
		t.Errorf("GET /metrics body missing geecache_hits_total:\n%s", body)
	}

	select {
	case err := <-serveErr:
		t.Fatalf("unexpected runtime error: %v", err)
	default:
	}
}

func TestStartAdminServerBindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy port: %v", err)
	}
	defer ln.Close()

	if _, err := startAdminServer(ln.Addr().String(), make(chan error, 1)); err == nil {
		t.Fatal("expected bind conflict error, got nil")
	}
}

// writeSelfSignedCert 生成一张供测试使用的自签名证书，返回证书和私钥路径。
func writeSelfSignedCert(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certFile = filepath.Join(dir, "node.crt")
	keyFile = filepath.Join(dir, "node.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}
