package geecache

import "geecache/consistenthash"

// updateRing 按节点差集增量更新一致性哈希环：
// 只 Remove 已下线节点、Add 新上线节点，避免每次拓扑变化都重建整环。
// ring 为 nil 时按 replicas 新建。调用方必须保证对 ring 的独占访问
// （HTTPPool/GRPCPool 都在各自的 p.mu 内调用）。
func updateRing(ring *consistenthash.Map, replicas int, oldPeers, newPeers []string) *consistenthash.Map {
	if ring == nil {
		ring = consistenthash.New(replicas, nil)
		ring.Add(newPeers...)
		return ring
	}

	newSet := make(map[string]struct{}, len(newPeers))
	for _, peer := range newPeers {
		newSet[peer] = struct{}{}
	}
	for _, peer := range oldPeers {
		if _, ok := newSet[peer]; !ok {
			ring.Remove(peer)
		}
	}

	oldSet := make(map[string]struct{}, len(oldPeers))
	for _, peer := range oldPeers {
		oldSet[peer] = struct{}{}
	}
	added := make([]string, 0, len(newPeers))
	for _, peer := range newPeers {
		if _, ok := oldSet[peer]; !ok {
			added = append(added, peer)
		}
	}
	// 批量 Add，只排序一次。
	if len(added) > 0 {
		ring.Add(added...)
	}
	return ring
}
