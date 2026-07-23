package geecache

import (
	"encoding/json"
	"log"
	"os"
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

type FileDiscovery struct {
	self     string //本节点地址（如 "http://localhost:8001"）
	filePath string //共享peers.json路径
	stopCh   chan struct{}
}

func NewFileDiscovery(self, filePath string) *FileDiscovery {
	return &FileDiscovery{
		self:     self,
		filePath: filePath,
		stopCh:   make(chan struct{}),
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

// Register 将当前节点地址写入 peers.json（去重合并写入）
func (d *FileDiscovery) Register(addr string) error {
	existing, _ := d.GetPeers() // 文件不存在，得到nil
	seen := make(map[string]bool)
	for _, p := range existing {
		seen[p] = true
	}
	seen[addr] = true //把当前节点加入集合

	var merged []string
	for p := range seen {
		merged = append(merged, p)
	}

	data, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(d.filePath, data, 0644)
}

// Watch 启动后台轮询：每 3 秒读一次文件，发现变化时回调 onChange
func (d *FileDiscovery) Watch(onChange func([]string)) {
	var lastPeers []string //保存上一次读的节点列表， 用来对比是否发生变更
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop() 

		if peers, err := d.GetPeers(); err == nil {
			onChange(peers) //执行回调， initialize nodes 
			lastPeers = peers
		}
		for {
			select {
			case <- d.stopCh: //收到关闭信号
				return 
			case <-ticker.C:
				peers, err := d.GetPeers()
				if err != nil {
					log.Printf("[FileDiscovery] read peers error: %v" err)
					continue 
				}
				if !sameSlice(peers, lastPeers) {
					log.Printf("\[FileDiscovery\] peers changed: %v", peers)
					onChange(peers)       // 节点变化！触发回调通知上层
					lastPeers = peers     // 更新基准列表
				}
			}
		}
	}()
}

// Close 停止后台 Watch 轮询
func (d *FileDiscovery) Close() {
	close(d.stopCh)
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
