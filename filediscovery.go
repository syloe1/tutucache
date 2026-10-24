package geecache

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// FileDiscovery — 基于 JSON 文件的服务发现（无外部依赖）
//
// peers.json 格式: ["http://localhost:8001", "http://localhost:8002", ...]
//
// 使用方式：
//
//	d := geecache.NewFileDiscovery("http://localhost:8001", "peers.json")
//	pool.StartDiscovery(d)
// =============================================================================

const (
	watchInterval    = 3 * time.Second // Watch 轮询间隔
	defaultHeartbeat = 3 * time.Second // 默认心跳重注册间隔
	lockFileSuffix   = ".lock"         // peers.json -> peers.json.lock
	renameRetries    = 5               // Windows 上 Rename 可能被短暂占用
	renameRetryDelay = 10 * time.Millisecond
)

// registerMu 串行化同一进程内的注册；跨进程互斥由 acquireFileLock 保证。
// 两个锁都需要：文件锁解决多节点并发，进程内互斥避免同进程 goroutine 抢同一把文件锁。
var registerMu sync.Mutex

type FileDiscovery struct {
	self              string //本节点地址（如 "http://localhost:8001"）
	filePath          string //共享peers.json路径
	stopCh            chan struct{}
	watchDone         chan struct{}
	watchOnce         sync.Once
	watchStarted      atomic.Bool
	closeOnce         sync.Once
	heartbeatInterval time.Duration //心跳重注册间隔（测试可调小）
}

func NewFileDiscovery(self, filePath string) *FileDiscovery {
	return &FileDiscovery{
		self:              self,
		filePath:          filePath,
		stopCh:            make(chan struct{}),
		watchDone:         make(chan struct{}),
		heartbeatInterval: defaultHeartbeat,
	}
}

// GetPeers 读取 peers.json，返回所有节点地址
func (d *FileDiscovery) GetPeers() ([]string, error) {
	data, err := os.ReadFile(d.filePath)
	if err != nil {
		return nil, err
	}
	var peers []string
	if err := json.Unmarshal(data, &peers); err != nil {
		return nil, err
	}
	return peers, nil
}

// Register 将当前节点地址写入 peers.json（去重合并写入）。
//
// 这是跨进程的读-改-写，必须满足：
//  1. 用文件锁把“读 -> 合并 -> 写”变成临界区，否则并发注册会 last-writer-wins；
//  2. 写临时文件后 os.Rename 原子替换，读者不会读到半截 JSON；
//  3. 排序后写盘，保证相同节点集合产生稳定的文件内容；
//  4. 读失败（文件损坏等）直接返回错误，绝不带着空列表写盘，
//     否则会把其它节点从文件里抹掉。
func (d *FileDiscovery) Register(addr string) error {
	registerMu.Lock()
	defer registerMu.Unlock()

	lock, err := acquireFileLock(d.filePath + lockFileSuffix)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer lock.release()

	existing, err := d.GetPeers()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		// 文件不存在 = 还没有节点注册，可以安全地从空列表开始；
		// 其它错误（权限、JSON 损坏）则宁可失败，也不能覆盖已有数据。
		return fmt.Errorf("read peers: %w", err)
	}

	merged := mergePeers(existing, addr)
	data, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n') // 文件以换行结尾，便于 diff/编辑

	return writeFileAtomic(d.filePath, data, 0o644)
}

// Unregister 从 peers.json 中移除指定节点地址 addr。
// 文件不存在时视为已经注销，直接返回 nil；文件损坏时返回错误且不覆盖原文件。
func (d *FileDiscovery) Unregister(addr string) error {
	registerMu.Lock()
	defer registerMu.Unlock()

	lock, err := acquireFileLock(d.filePath + lockFileSuffix)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer lock.release()

	existing, err := d.GetPeers()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read peers: %w", err)
	}

	filtered, removed := removePeer(existing, addr)
	if !removed {
		return nil // 地址不在列表中，无需写盘
	}
	data, err := json.MarshalIndent(filtered, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(d.filePath, data, 0o644)
}

// removePeer 删除切片中等于 addr 的元素，去重并排序。
// 第二个返回值表示是否真的删除了 addr。
func removePeer(list []string, addr string) ([]string, bool) {
	seen := make(map[string]struct{}, len(list))
	removed := false
	for _, p := range list {
		if p == "" {
			continue
		}
		if p == addr {
			removed = true
			continue
		}
		seen[p] = struct{}{}
	}
	res := make([]string, 0, len(seen))
	for p := range seen {
		res = append(res, p)
	}
	sort.Strings(res)
	return res, removed
}

// Watch 启动后台轮询：每 3 秒读一次文件，发现变化时回调 onChange。
// 同时启动心跳，周期性地重新注册自己：即使某次写入因进程异常或外部覆盖而丢失，
// 节点列表也会在几秒内自愈，而不是永久停留在残缺状态。
// 每个 FileDiscovery 只会启动一个 Watch goroutine，重复调用会被忽略。
func (d *FileDiscovery) Watch(onChange func([]string)) {
	d.watchOnce.Do(func() {
		d.watchStarted.Store(true)
		go d.watch(onChange)
	})
}

func (d *FileDiscovery) watch(onChange func([]string)) {
	defer close(d.watchDone)

	// Close 可能先于 Watch 执行；此时不需要再读取文件或触发回调。
	select {
	case <-d.stopCh:
		return
	default:
	}

	var lastPeers []string // 保存上一次读的节点列表，用来对比是否发生变更

	watchTicker := time.NewTicker(watchInterval)
	defer watchTicker.Stop()

	heartbeatInterval := d.heartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeat
	}
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	if peers, err := d.GetPeers(); err == nil {
		onChange(peers) //执行回调， initialize nodes
		lastPeers = peers
	}

	for {
		select {
		case <-d.stopCh: //收到关闭信号
			return
		case <-watchTicker.C:
			peers, err := d.GetPeers()
			if err != nil {
				log.Printf("[FileDiscovery] read peers error: %v", err)
				continue
			}
			if !sameSlice(peers, lastPeers) {
				log.Printf("[FileDiscovery] peers changed: %v", peers)
				onChange(peers)   // 节点变化！触发回调通知上层
				lastPeers = peers // 更新基准列表
			}
		case <-heartbeatTicker.C:
			if err := d.Register(d.self); err != nil {
				log.Printf("[FileDiscovery] re-register %s error: %v", d.self, err)
			}
		}
	}
}

// Close 停止后台 Watch 轮询并等待 goroutine 退出；可重复调用。
// 如果 Watch 从未启动，Close 不会等待（watchDone 永远不会被关闭）。
// 返回后不会再有心跳注册：这是 Unregister 之前必须调用 Close 的原因。
func (d *FileDiscovery) Close() {
	d.closeOnce.Do(func() {
		close(d.stopCh)
		if d.watchStarted.Load() {
			<-d.watchDone
		}
	})
}

// mergePeers 合并已有节点与当前节点，去重并排序。
func mergePeers(existing []string, addr string) []string {
	seen := make(map[string]struct{}, len(existing)+1)
	for _, p := range existing {
		if p != "" {
			seen[p] = struct{}{}
		}
	}
	if addr != "" {
		seen[addr] = struct{}{}
	}

	merged := make([]string, 0, len(seen))
	for p := range seen {
		merged = append(merged, p)
	}
	sort.Strings(merged)
	return merged
}

// writeFileAtomic 在同目录创建临时文件，写完后 Rename 覆盖目标文件。
// Rename 在同一文件系统内是原子的：读者要么看到旧文件，要么看到新文件。
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	// Rename 成功后临时文件已不存在，Remove 的 ErrNotExist 可以忽略。
	defer os.Remove(tmpName)

	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	for i := 0; i < renameRetries; i++ {
		if err := os.Rename(tmpName, path); err == nil {
			return nil
		} else if i == renameRetries-1 {
			return fmt.Errorf("rename temp file: %w", err)
		}
		// Windows 上目标文件可能被读者/杀毒软件短暂占用，退避后重试。
		time.Sleep(renameRetryDelay * time.Duration(i+1))
	}
	return nil // 循环内已返回，不会到达
}

func sameSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]bool, len(a))
	for _, s := range a {
		m[s] = true
	}
	for _, s := range b {
		if !m[s] {
			return false
		}
	}
	return true
}
