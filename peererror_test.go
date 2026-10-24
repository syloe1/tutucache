package geecache

import (
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 回归 #17：key 不存在必须是可识别的「正常未命中」，覆盖 HTTP、gRPC
// 以及同进程 Getter 直接返回哨兵错误三种来源。
func TestIsPeerKeyNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"sentinel", ErrKeyNotFound, true},
		{"wrapped sentinel", fmt.Errorf("db: %w", ErrKeyNotFound), true},
		{"http gone", &PeerHTTPError{StatusCode: http.StatusGone}, true},
		{"wrapped http gone", fmt.Errorf("peer: %w", &PeerHTTPError{StatusCode: http.StatusGone}), true},
		{"grpc not found", status.Error(codes.NotFound, "key not found"), true},

		// 404 是「Group 未注册」的配置错误，不能当成 key 不存在。
		{"http 404 is config", &PeerHTTPError{StatusCode: http.StatusNotFound}, false},
		{"http 500", &PeerHTTPError{StatusCode: http.StatusInternalServerError}, false},
		{"grpc internal", status.Error(codes.Internal, "boom"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPeerKeyNotFound(tt.err); got != tt.want {
				t.Fatalf("isPeerKeyNotFound(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// 配置错误在两种传输下都要能被识别，且不能与 key 不存在混淆。
func TestIsPeerConfigError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"http 401", &PeerHTTPError{StatusCode: http.StatusUnauthorized}, true},
		{"http 403", &PeerHTTPError{StatusCode: http.StatusForbidden}, true},
		{"http 404 group", &PeerHTTPError{StatusCode: http.StatusNotFound}, true},
		{"http 410 key not found", &PeerHTTPError{StatusCode: http.StatusGone}, false},
		{"http 500", &PeerHTTPError{StatusCode: http.StatusInternalServerError}, false},

		{"grpc unauthenticated", status.Error(codes.Unauthenticated, "bad token"), true},
		{"grpc permission denied", status.Error(codes.PermissionDenied, "forbidden"), true},
		{"grpc failed precondition", status.Error(codes.FailedPrecondition, "no such group"), true},
		{"grpc not found", status.Error(codes.NotFound, "key not found"), false},
		{"grpc internal", status.Error(codes.Internal, "boom"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPeerConfigError(tt.err); got != tt.want {
				t.Fatalf("isPeerConfigError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
