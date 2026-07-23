import subprocess
import time
import os
import signal
import sys

# 保存所有子进程，退出时统一杀死
process_list = []

def cleanup(signum=None, frame=None):
    print("\n[Cleanup] 关闭所有服务进程...")
    for p in process_list:
        try:
            if p.poll() is None:
                p.terminate()           # 发送 SIGTERM，触发优雅关闭
                p.wait(timeout=12)      # 等待服务完成正在处理的请求
        except subprocess.TimeoutExpired:
            p.kill()                    # 超时强制杀死
        except Exception:
            pass
    # Windows 强制杀残留 server.exe
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/IM", "server.exe"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # 删除编译产物
    if os.path.exists("server.exe"):
        os.remove("server.exe")
    print("[Cleanup] done")
    sys.exit(0)

# 捕获 Ctrl+C / 正常退出信号
signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

def main():
    # 1. 清理旧文件
    if os.path.exists("server.exe"):
        os.remove("server.exe")

    # 2. 编译 Go 程序
    print("[Build] 编译 server.exe ...")
    build_cmd = ["go", "build", "-o", "server.exe", "./cmd"]
    ret = subprocess.run(build_cmd)
    if ret.returncode != 0 or not os.path.exists("server.exe"):
        print("[Error] Go 编译失败，请检查 main.go 入口文件")
        return

    # 3. 启动3个后台服务
    if sys.platform == "win32":
        flags = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    else:
        flags = {}

    print("[Start] 启动节点 8001")
    p1 = subprocess.Popen(["./server.exe", "-port=8001"], **flags)
    process_list.append(p1)

    print("[Start] 启动节点 8002")
    p2 = subprocess.Popen(["./server.exe", "-port=8002"], **flags)
    process_list.append(p2)

    print("[Start] 启动网关节点 8003 (9999 API)")
    p3 = subprocess.Popen(["./server.exe", "-port=8003", "-api=1"], **flags)
    process_list.append(p3)

    # 等待所有节点健康就绪
    import urllib.request
    print("[Wait] 等待所有节点就绪...")
    for port in [8001, 8002, 8003]:
        ready = False
        for _ in range(30):  # 最多等 6 秒
            try:
                urllib.request.urlopen(f"http://localhost:{port}/health", timeout=0.5)
                ready = True
                break
            except Exception:
                time.sleep(0.2)
        if ready:
            print(f"[OK] :{port} is healthy")
        else:
            print(f"[Warn] :{port} health check timeout")

    # 4. 并发 curl 测试
    print(">>> start test curl http://localhost:9999/api?key=Tom")
    curl_cmds = [
        ["curl", "http://localhost:9999/api?key=Tom"],
        ["curl", "http://localhost:9999/api?key=Tom"],
        ["curl", "http://localhost:9999/api?key=Tom"],
    ]
    curl_procs = []
    for cmd in curl_cmds:
        cp = subprocess.Popen(cmd)
        curl_procs.append(cp)
    # 等待所有curl请求完成
    for cp in curl_procs:
        cp.wait()

    print("[Test Done] 所有请求执行完毕，按 Ctrl+C 关闭服务")
    # 阻塞等待，直到用户 Ctrl+C
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
