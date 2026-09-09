#!/bin/bash
# trap: 收到退出信号时，清理产物并优雅关闭所有子进程
cleanup() {
    echo '[stop] shutting down...'
    # 先删产物（运行中的进程已把二进制加载进内存，可安全删除）
    rm -f server peers.json
    # 给后台服务发 SIGTERM 触发优雅关闭（只杀子进程，不杀脚本自身）
    local pids
    pids=$(jobs -p)
    if [ -n "$pids" ]; then
        kill $pids 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    echo '[stop] done'
}
trap cleanup EXIT

set -e

echo "[build] compiling..."
go build -o server ./cmd

# 预创建 peers.json，避免 3 个节点并发写文件导致覆盖
echo '[build] creating peers.json...'
cat > peers.json <<'EOF'
[
  "http://localhost:8001",
  "http://localhost:8002",
  "http://localhost:8003"
]
EOF

echo "[start] launching 3 nodes..."
./server -port=8001 &
./server -port=8002 &
./server -port=8003 -api=1 &

# 等待所有节点健康检查通过（替代原来的 sleep 2）
for port in 8001 8002 8003; do
    echo "[wait] waiting for :$port ready..."
    until curl -s -o /dev/null http://localhost:$port/health; do
        sleep 0.2
    done
    echo "[ok] :$port is healthy"
done

echo ">>> start test"
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &

wait
