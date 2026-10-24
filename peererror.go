package geecache

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// =============================================================================
// peer 错误分类 —— 与传输层无关
//
// 上层只关心错误的语义，而不是它来自 HTTP 还是 gRPC：
//   config      集群配置不一致（授权失败 / Group 未注册），必须暴露给调用方
//   notFound    对端明确表示 key 不存在，属于正常未命中，不能计入熔断
//   其他        远端临时故障，触发熔断并降级本地
//
// 两种传输使用互不冲突的状态码，保证「key 不存在」不会被误判为配置错误。
// =============================================================================

// isPeerConfigError 判断是否为集群配置错误。
// HTTP: 401/403/404（404 专指 Group 未注册）；gRPC: 授权失败或 Group 未注册。
func isPeerConfigError(err error) bool {
	var peerErr *PeerHTTPError
	if errors.As(err, &peerErr) {
		return peerErr.IsConfigError()
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

// isPeerKeyNotFound 判断对端是否明确表示 key 不存在。
// 同进程内 Getter 直接返回 ErrKeyNotFound 的情况也在这里统一处理。
func isPeerKeyNotFound(err error) bool {
	if errors.Is(err, ErrKeyNotFound) {
		return true
	}
	var peerErr *PeerHTTPError
	if errors.As(err, &peerErr) {
		return peerErr.IsKeyNotFound()
	}
	return status.Code(err) == codes.NotFound
}
