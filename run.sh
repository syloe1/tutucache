#!/bin/bash
cleanup() {
    local pids
    pids=$(jobs -p)
    if [ -n "$pids" ]; then
        kill $pids 2>/dev/null || true
        wait 2>/dev/null || true
    fi
    rm -f server
    # peers.json 不在这里删除，交给节点的 Unregister，方便验证注销结果。
}
trap cleanup EXIT SIGINT SIGTERM

set -e

echo "[build] compiling..."
go build -o server ./cmd

# peers.json 由各节点启动时自行并发注册，Register 内部有跨进程文件锁，
# 不需要预创建（保留 peers.json.lock 是正常的，锁文件不删除以避免竞争）。
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
