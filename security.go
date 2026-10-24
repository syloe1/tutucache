package geecache

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// tokenEqual 以恒定时间比较两个共享 token。
//
// 先分别取 SHA-256 摘要再比较：两边都是固定 32 字节，不会像字符串 != 那样
// 在前缀不匹配时提前返回；取摘要也避免了 ConstantTimeCompare 在长度不同时
// 提前返回，降低通过响应时间逐字节猜测 token 的风险。
func tokenEqual(got, want string) bool {
	gotHash := sha256.Sum256([]byte(got))
	wantHash := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(gotHash[:], wantHash[:]) == 1
}

// buildPeerTLSConfigs 加载节点证书，构造节点间通信所需的服务端/客户端配置。
//
// 两个角色必须使用独立的 *tls.Config：服务端需要 ClientCAs + ClientAuth
// 校验客户端证书，客户端需要 RootCAs 校验服务端证书。
// caCertFile 为空时只加密、不要求客户端证书；非空时开启 mTLS。
//
// 同一个证书同时用于服务端和客户端角色，因此证书 SAN 必须覆盖 peer 实际
// 拨号的地址；启用 mTLS 时证书还需要 clientAuth EKU。
func buildPeerTLSConfigs(certFile, keyFile, caCertFile string) (serverCfg, clientCfg *tls.Config, err error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load cert: %w", err)
	}

	serverCfg = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	clientCfg = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if caCertFile != "" {
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, nil, errors.New("append CA cert: invalid PEM")
		}
		// 服务端用 ClientCAs 校验客户端证书。
		serverCfg.ClientCAs = caCertPool
		serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
		// 客户端用 RootCAs 校验远端节点的服务端证书。
		clientCfg.RootCAs = caCertPool
	}

	return serverCfg, clientCfg, nil
}
