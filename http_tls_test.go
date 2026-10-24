package geecache

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "geecache/geecachepb"
)

var testSerial atomic.Int64

var bothEKUs = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

type issuedCert struct {
	certFile string
	keyFile  string
	tlsCert  tls.Certificate
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(testSerial.Add(1)),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return &testCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func (ca *testCA) writePEM(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(path, ca.pem, 0o600); err != nil {
		t.Fatalf("write CA cert: %v", err)
	}
	return path
}

func (ca *testCA) certPool(t *testing.T) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.pem) {
		t.Fatal("append CA cert to pool")
	}
	return pool
}

func (ca *testCA) issueCert(t *testing.T, dir, name string, ekus []x509.ExtKeyUsage, dns []string, ips []net.IP) issuedCert {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate %s key: %v", name, err)
	}
	serial := testSerial.Add(1)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  ekus,
		DNSNames:     dns,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create %s certificate: %v", name, err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", name, err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certFile := filepath.Join(dir, fmt.Sprintf("%s-%d.crt", name, serial))
	keyFile := filepath.Join(dir, fmt.Sprintf("%s-%d.key", name, serial))
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write %s cert: %v", name, err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write %s key: %v", name, err)
	}
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("parse %s key pair: %v", name, err)
	}
	return issuedCert{certFile: certFile, keyFile: keyFile, tlsCert: tlsCert}
}

func newTestTLSPool(t *testing.T, self, caFile string, node issuedCert) *HTTPPool {
	t.Helper()
	p := NewHTTPPool(self)
	if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err != nil {
		t.Fatalf("EnableTLS: %v", err)
	}
	return p
}

func testNodeCert(t *testing.T, ca *testCA, dir string) issuedCert {
	t.Helper()
	return ca.issueCert(t, dir, "node", bothEKUs,
		[]string{"localhost"},
		[]net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	)
}

func TestEnableTLSSplitsServerAndClientConfigs(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := newTestTLSPool(t, "https://localhost:8001", ca.writePEM(t, dir), node)

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
		t.Error("server config must not use RootCAs (that is a client-side setting)")
	}
	if server.MinVersion != tls.VersionTLS12 {
		t.Errorf("server MinVersion = %x, want %x", server.MinVersion, tls.VersionTLS12)
	}
	if len(server.Certificates) != 1 {
		t.Errorf("server certificates = %d, want 1", len(server.Certificates))
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
	if len(client.Certificates) != 1 {
		t.Errorf("client certificates = %d, want 1", len(client.Certificates))
	}
	if client == server {
		t.Error("server and client must not share the same tls.Config")
	}
}

func TestEnableTLSWithoutCAOnlyEncrypts(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := newTestTLSPool(t, "https://localhost:8001", "", node)

	server := p.TLSConfig()
	if server.ClientAuth != tls.NoClientCert {
		t.Errorf("ClientAuth = %v, want NoClientCert", server.ClientAuth)
	}
	if server.ClientCAs != nil {
		t.Error("ClientCAs must be nil when no CA is configured")
	}
	if p.clientTLSConfig.RootCAs != nil {
		t.Error("RootCAs must be nil when no CA is configured")
	}
}

func TestEnableTLSRejectsInvalidCA(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	caFile := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(caFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write invalid CA: %v", err)
	}

	p := NewHTTPPool("https://localhost:8001")
	if err := p.EnableTLS(node.certFile, node.keyFile, caFile); err == nil {
		t.Fatal("EnableTLS() = nil error for invalid CA PEM")
	}
}

func TestSetNormalizesPeerSchemeWhenTLSEnabled(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := newTestTLSPool(t, "http://self:8001", ca.writePEM(t, dir), node)
	p.Set("http://self:8001", "http://a:8001", "https://a:8001", "http://b:8002")

	if len(p.httpGetters) != 2 {
		t.Fatalf("httpGetters = %d, want 2 (self skipped, duplicate a deduped)", len(p.httpGetters))
	}
	for _, peer := range []string{"https://a:8001", "https://b:8002"} {
		getter, ok := p.httpGetters[peer]
		if !ok {
			t.Errorf("missing getter for %s", peer)
			continue
		}
		wantBase := peer + defaultBasePath
		if getter.baseURL != wantBase {
			t.Errorf("getter baseURL = %q, want %q", getter.baseURL, wantBase)
		}
		transport, ok := getter.httpClient.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("transport type = %T, want *http.Transport", getter.httpClient.Transport)
		}
		if transport.TLSClientConfig != p.clientTLSConfig {
			t.Errorf("getter for %s does not use the client TLS config", peer)
		}
	}
	if _, ok := p.httpGetters["https://self:8001"]; ok {
		t.Error("self getter must not be created")
	}
	wantPeers := []string{"https://self:8001", "https://a:8001", "https://b:8002"}
	if !sameSlice(p.peersList, wantPeers) {
		t.Errorf("peersList = %v, want %v", p.peersList, wantPeers)
	}
}

func TestSetKeepsHTTPWhenTLSDisabled(t *testing.T) {
	p := NewHTTPPool("http://self:8001")
	p.Set("https://a:8001")

	getter, ok := p.httpGetters["http://a:8001"]
	if !ok {
		t.Fatalf("missing http getter, got keys %v", p.httpGetters)
	}
	if getter.baseURL != "http://a:8001"+defaultBasePath {
		t.Errorf("baseURL = %q, want http://a:8001%s", getter.baseURL, defaultBasePath)
	}
	if transport := getter.httpClient.Transport.(*http.Transport); transport.TLSClientConfig != nil {
		t.Error("TLSClientConfig must be nil when TLS is disabled")
	}
}

func TestSetRebuildsGettersWhenTLSEnabledLater(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	p := NewHTTPPool("http://self:8001")
	p.Set("http://a:8001")
	oldGetter := p.httpGetters["http://a:8001"]
	if oldGetter == nil {
		t.Fatal("missing initial getter")
	}
	tr := replaceTransport(oldGetter)

	if err := p.EnableTLS(node.certFile, node.keyFile, ca.writePEM(t, dir)); err != nil {
		t.Fatalf("EnableTLS: %v", err)
	}
	p.Set("http://a:8001")

	newGetter := p.httpGetters["https://a:8001"]
	if newGetter == nil {
		t.Fatalf("missing https getter, got keys %v", p.httpGetters)
	}
	if newGetter == oldGetter {
		t.Fatal("getter was not rebuilt after TLS was enabled")
	}
	if _, ok := p.httpGetters["http://a:8001"]; ok {
		t.Error("stale http getter is still registered")
	}
	if n := tr.closeIdle.Load(); n != 1 {
		t.Errorf("closeIdle called %d times for stale getter, want 1", n)
	}
}

func TestTLSGetterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	caFile := ca.writePEM(t, dir)
	node := testNodeCert(t, ca, dir)

	const groupName = "tls-roundtrip"
	NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("remote:" + key), nil
	}))

	serverPool := newTestTLSPool(t, "https://localhost", caFile, node)
	srv := httptest.NewUnstartedServer(serverPool)
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.TLS = serverPool.TLSConfig()
	srv.StartTLS()
	defer srv.Close()
	if !strings.HasPrefix(srv.URL, "https://") {
		t.Fatalf("httptest server URL = %q, want https://", srv.URL)
	}

	clientPool := newTestTLSPool(t, "https://client", caFile, node)
	getter := newHTTPGetter(srv.URL+defaultBasePath, clientPool.clientTLSConfig, "")

	out := &pb.Response{}
	if err := getter.Get(&pb.Request{Group: groupName, Key: "Tom"}, out); err != nil {
		t.Fatalf("TLS get: %v", err)
	}
	if got, want := string(out.Value), "remote:Tom"; got != want {
		t.Errorf("value = %q, want %q", got, want)
	}
}

func TestTLSWithoutCADoesNotRequireClientCert(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	node := testNodeCert(t, ca, dir)

	serverPool := newTestTLSPool(t, "https://localhost", "", node)
	var hits atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.TLS = serverPool.TLSConfig()
	srv.StartTLS()
	defer srv.Close()

	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs:    ca.certPool(t),
			MinVersion: tls.VersionTLS12,
		}},
		Timeout: 2 * time.Second,
	}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("client without certificate rejected in TLS-only mode: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if hits.Load() != 1 {
		t.Errorf("handler hits = %d, want 1", hits.Load())
	}
}

func TestMTLSClientCertificateVerification(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	caFile := ca.writePEM(t, dir)
	node := testNodeCert(t, ca, dir)

	serverPool := newTestTLSPool(t, "https://localhost", caFile, node)
	var hits atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.TLS = serverPool.TLSConfig()
	srv.StartTLS()
	defer srv.Close()

	caPool := ca.certPool(t)
	newClient := func(certs []tls.Certificate) *http.Client {
		return &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs:      caPool,
				Certificates: certs,
				MinVersion:   tls.VersionTLS12,
			}},
			Timeout: 2 * time.Second,
		}
	}

	t.Run("no client cert rejected", func(t *testing.T) {
		before := hits.Load()
		resp, err := newClient(nil).Get(srv.URL)
		if err == nil {
			resp.Body.Close()
			t.Fatal("expected TLS handshake failure for client without certificate")
		}
		if got := hits.Load(); got != before {
			t.Errorf("handler ran %d time(s) for unauthenticated client", got-before)
		}
	})

	t.Run("untrusted client cert rejected", func(t *testing.T) {
		otherCA := newTestCA(t)
		rogue := otherCA.issueCert(t, dir, "rogue",
			[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)

		before := hits.Load()
		resp, err := newClient([]tls.Certificate{rogue.tlsCert}).Get(srv.URL)
		if err == nil {
			resp.Body.Close()
			t.Fatal("expected certificate verification failure for untrusted client cert")
		}
		if got := hits.Load(); got != before {
			t.Errorf("handler ran %d time(s) for untrusted client", got-before)
		}
	})

	t.Run("trusted client cert accepted", func(t *testing.T) {
		before := hits.Load()
		resp, err := newClient([]tls.Certificate{node.tlsCert}).Get(srv.URL)
		if err != nil {
			t.Fatalf("trusted client rejected: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
		if got := hits.Load(); got != before+1 {
			t.Errorf("handler hits = %d, want %d", got, before+1)
		}
	})
}
