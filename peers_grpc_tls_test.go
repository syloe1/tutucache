package geecache

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync/atomic"
	"testing"

	pb "geecache/geecachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
)

func newGRPCTLSPool(t *testing.T, self, caFile string, node issuedCert) *GRPCPool {
	t.Helper()
	p := NewGRPCPool(self)
	if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
		t.Fatalf("EnableTLS: %v", err)
	}
	return p
}

func TestGRPCEnableTLSSplitsServerAndClientConfigs(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := newGRPCTLSPool(t, "localhost:8001", ca.writePEM(t, dir), node)
	defer p.Stop()

	server := p.TLSConfig()
	if server == nil {
		t.Fatal("server TLS config is nil")
	}
	if server.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", server.ClientAuth)
	}
	if server.ClientCAs == nil {
		t.Error("ClientCAs is nil, server cannot verify client certificates")
	}
	if server.RootCAs != nil {
		t.Error("server config must not use RootCAs")
	}
	if server.MinVersion != tls.VersionTLS12 {
		t.Errorf("server MinVersion = %x, want %x", server.MinVersion, tls.VersionTLS12)
	}

	client := p.clientTLSConfig
	if client == nil {
		t.Fatal("client TLS config is nil")
	}
	if client.RootCAs == nil {
		t.Error("client RootCAs is nil, peer server certificates cannot be verified")
	}
	if client.ClientAuth != tls.NoClientCert {
		t.Errorf("client ClientAuth = %v, want NoClientCert", client.ClientAuth)
	}
	if client == server {
		t.Error("server and client must not share the same tls.Config")
	}
}

func TestGRPCEnableTLSWithoutCAOnlyEncrypts(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := newGRPCTLSPool(t, "localhost:8001", "", node)
	defer p.Stop()

	if got := p.TLSConfig().ClientAuth; got != tls.NoClientCert {
		t.Errorf("ClientAuth = %v, want NoClientCert", got)
	}
	if p.TLSConfig().ClientCAs != nil {
		t.Error("ClientCAs must be nil when no CA is configured")
	}
	if p.clientTLSConfig.RootCAs != nil {
		t.Error("RootCAs must be nil when no CA is configured")
	}
}

func TestGRPCEnableTLSRejectsInvalidFiles(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := NewGRPCPool("localhost:8001")
	defer p.Stop()

	if err := p.EnableTLS("/nonexistent/cert.pem", "/nonexistent/key.pem", ""); err == nil {
		t.Fatal("EnableTLS() = nil error for missing cert files")
	}

	badCA := dir + "/bad-ca.pem"
	if err := os.WriteFile(badCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write invalid CA: %v", err)
	}
	if err := p.EnableTLS(node.certFile, node.keyFile, badCA); err == nil {
		t.Fatal("EnableTLS() = nil error for invalid CA PEM")
	}
	if p.TLSConfig() != nil || p.clientTLSConfig != nil {
		t.Error("failed EnableTLS must not leave partial TLS config")
	}
}

// EnableTLS 在 Set 之后调用时，已有 peer 的连接必须用新配置重建，
// 旧连接关闭，避免继续用明文发送请求。
func TestGRPCEnableTLSRebuildsGettersImmediately(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := NewGRPCPool("localhost:9000")
	defer p.Stop()
	p.Set("localhost:9001")

	oldGetter := p.grpcGetters["localhost:9001"]
	if oldGetter == nil {
		t.Fatal("missing plaintext getter")
	}

	if err := p.EnableTLS(node.certFile, node.keyFile, ca.writePEM(t, dir)); err != nil {
		t.Fatalf("EnableTLS: %v", err)
	}

	newGetter := p.grpcGetters["localhost:9001"]
	if newGetter == nil || newGetter == oldGetter {
		t.Fatal("getter was not rebuilt after enabling TLS")
	}
	if newGetter.clientTLSConfig != p.clientTLSConfig {
		t.Error("rebuilt getter does not use the client TLS config")
	}
	if got := oldGetter.conn.GetState(); got != connectivity.Shutdown {
		t.Errorf("old conn state = %v, want Shutdown", got)
	}
}

// gRPC server 的凭据创建后不可替换，启动后调用 EnableTLS 必须返回错误。
func TestGRPCEnableTLSAfterServeFails(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	server, _ := startTestGRPCServerPool(t, nil)
	if err := server.EnableTLS(node.certFile, node.keyFile, ca.writePEM(t, dir)); err == nil {
		t.Fatal("EnableTLS() after ServeListener = nil error, want error")
	}
}

func TestGRPCTLSRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	caFile := ca.writePEM(t, dir)
	node := testNodeCert(t, ca, dir)

	const groupName = "grpc-tls-roundtrip"
	NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("tls:" + key), nil
	}))

	_, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
		if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
			t.Fatalf("server EnableTLS: %v", err)
		}
	})

	client := newTestGRPCClientPool(t, func(p *GRPCPool) {
		if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
			t.Fatalf("client EnableTLS: %v", err)
		}
	}, addr)

	out := &pb.Response{}
	if err := testGRPCGetter(t, client, addr).Get(&pb.Request{Group: groupName, Key: "Tom"}, out); err != nil {
		t.Fatalf("TLS Get: %v", err)
	}
	if got, want := string(out.Value), "tls:Tom"; got != want {
		t.Errorf("value = %q, want %q", got, want)
	}
}

// TLS-only 模式（未配置 CA）不要求客户端证书，但客户端仍需信任服务端证书。
func TestGRPCTLSWithoutCADoesNotRequireClientCert(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	const groupName = "grpc-tls-only"
	NewGroup(groupName, 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("encrypted"), nil
	}))

	_, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
		if err := p.EnableTLS(node.certFile, node.keyFile, ""); err != nil {
			t.Fatalf("server EnableTLS: %v", err)
		}
	})

	conn := dialTestGRPC(t, addr, credentials.NewTLS(&tls.Config{
		RootCAs:    ca.certPool(t),
		MinVersion: tls.VersionTLS12,
	}))
	resp, err := pb.NewGroupCacheClient(conn).Get(context.Background(),
		&pb.Request{Group: groupName, Key: "k"})
	if err != nil {
		t.Fatalf("TLS client without certificate rejected: %v", err)
	}
	if got := string(resp.Value); got != "encrypted" {
		t.Errorf("value = %q, want encrypted", got)
	}
}

func TestGRPCTLSRejectsPlaintextAndUntrustedPeers(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	caFile := ca.writePEM(t, dir)
	node := testNodeCert(t, ca, dir)

	const groupName = "grpc-tls-reject"
	NewGroup(groupName, 1<<20, GetterFunc(func(string) ([]byte, error) {
		return []byte("secret"), nil
	}))

	t.Run("plaintext client to TLS server", func(t *testing.T) {
		_, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
			if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
				t.Fatalf("server EnableTLS: %v", err)
			}
		})
		client := newTestGRPCClientPool(t, nil, addr)
		err := testGRPCGetter(t, client, addr).Get(&pb.Request{Group: groupName, Key: "k"}, &pb.Response{})
		if err == nil {
			t.Fatal("plaintext client reached TLS server")
		}
		assertNotConfigError(t, err)
	})

	t.Run("TLS client to plaintext server", func(t *testing.T) {
		_, addr := startTestGRPCServerPool(t, nil)
		client := newTestGRPCClientPool(t, func(p *GRPCPool) {
			if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
				t.Fatalf("client EnableTLS: %v", err)
			}
		}, addr)
		err := testGRPCGetter(t, client, addr).Get(&pb.Request{Group: groupName, Key: "k"}, &pb.Response{})
		if err == nil {
			t.Fatal("TLS client reached plaintext server")
		}
		assertNotConfigError(t, err)
	})

	t.Run("untrusted server certificate", func(t *testing.T) {
		_, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
			if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
				t.Fatalf("server EnableTLS: %v", err)
			}
		})

		otherDir := t.TempDir()
		otherCA := newTestCA(t)
		otherNode := testNodeCert(t, otherCA, otherDir)
		client := newTestGRPCClientPool(t, func(p *GRPCPool) {
			if err := p.EnableTLS(otherNode.certFile, otherNode.keyFile, otherCA.writePEM(t, otherDir)); err != nil {
				t.Fatalf("client EnableTLS: %v", err)
			}
		}, addr)

		err := testGRPCGetter(t, client, addr).Get(&pb.Request{Group: groupName, Key: "k"}, &pb.Response{})
		if err == nil {
			t.Fatal("client trusted a certificate from an unrelated CA")
		}
		assertNotConfigError(t, err)
	})
}

func TestGRPCMTLSClientCertificateVerification(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	caFile := ca.writePEM(t, dir)
	node := testNodeCert(t, ca, dir)

	const groupName = "grpc-mtls"
	var handlerCalls atomic.Int32
	NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		handlerCalls.Add(1)
		return []byte("mtls:" + key), nil
	}))

	_, addr := startTestGRPCServerPool(t, func(p *GRPCPool) {
		if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
			t.Fatalf("server EnableTLS: %v", err)
		}
	})
	caPool := ca.certPool(t)

	call := func(t *testing.T, certs []tls.Certificate) error {
		t.Helper()
		conn := dialTestGRPC(t, addr, credentials.NewTLS(&tls.Config{
			RootCAs:      caPool,
			Certificates: certs,
			MinVersion:   tls.VersionTLS12,
		}))
		_, err := pb.NewGroupCacheClient(conn).Get(context.Background(),
			&pb.Request{Group: groupName, Key: "k"})
		return err
	}

	t.Run("no client certificate rejected", func(t *testing.T) {
		before := handlerCalls.Load()
		if err := call(t, nil); err == nil {
			t.Fatal("server accepted a client without a certificate")
		}
		if got := handlerCalls.Load(); got != before {
			t.Errorf("handler ran %d time(s) for unauthenticated client", got-before)
		}
	})

	t.Run("untrusted client certificate rejected", func(t *testing.T) {
		otherCA := newTestCA(t)
		rogue := otherCA.issueCert(t, dir, "rogue",
			[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)

		before := handlerCalls.Load()
		if err := call(t, []tls.Certificate{rogue.tlsCert}); err == nil {
			t.Fatal("server accepted a client certificate from an unrelated CA")
		}
		if got := handlerCalls.Load(); got != before {
			t.Errorf("handler ran %d time(s) for untrusted client", got-before)
		}
	})

	t.Run("trusted client certificate accepted", func(t *testing.T) {
		before := handlerCalls.Load()
		if err := call(t, []tls.Certificate{node.tlsCert}); err != nil {
			t.Fatalf("trusted client rejected: %v", err)
		}
		if got := handlerCalls.Load(); got != before+1 {
			t.Errorf("handler calls = %d, want %d", got, before+1)
		}
	})
}

func assertNotConfigError(t *testing.T, err error) {
	t.Helper()
	if isPeerConfigError(err) {
		t.Errorf("transport failure %v was misclassified as a config error (should fall back locally)", err)
	}
}

// dialTestGRPC 建立一个测试用连接；连接是惰性的，握手错误在 RPC 时返回。
func dialTestGRPC(t *testing.T, addr string, creds credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s): %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
