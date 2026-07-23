#!/bin/bash
# trap: 收到 Ctrl+C 时，发 SIGTERM 给所有子进程并等待优雅退出
trap "echo '[stop] shutting down...'; kill 0; wait; rm -f server; echo '[stop] done'" EXIT

set -e

echo "[build] compiling..."
go build -o server ./cmd

echo "[start] launching 3 nodes..."
./server -port=8001 &
./server -port=8002 &
./server -port=8003 -api=1 &

sleep 1

# 等待所有节点健康检查通过
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
