package escheduler

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Hash func(data []byte) uint32

func NewHashRing(replicas int, fn Hash) *HashRing {
	r := &HashRing{
		replicas: replicas,
		hash:     fn,
		nodes:    make(map[int]string),
	}
	if r.hash == nil {
		r.hash = crc32.ChecksumIEEE
	}
	return r
}

type HashRing struct {
	hash     Hash
	replicas int   // Number of virtual nodes per real node
	ring     []int // Hash ring, sorted by node hash
	mu       sync.RWMutex
	nodes    map[int]string // node hash to real node string, inverse process of hash mapping
}

// Add new nodes to the hash ring
// Note that if the added node already exists, it will be duplicated above the hash ring, use Reset if you're not sure if it exists.
func (r *HashRing) addLocked(nodes ...string) {
	for _, node := range nodes {
		// Create multiple virtual nodes per node
		for i := 0; i < r.replicas; i++ {
			// 每个虚拟节点计算哈希值
			hash := int(r.hash([]byte(strconv.Itoa(i) + node)))
			// 加入哈希环
			r.ring = append(r.ring, hash)
			// 哈希值到真实节点字符串映射
			r.nodes[hash] = node
		}
	}
	// 哈希环排序
	sort.Ints(r.ring)
}

// Empty Whether there are nodes on the hash ring
func (r *HashRing) Empty() bool {
	return len(r.ring) == 0
}

// Empty the hash ring first and then set it
func (r *HashRing) Reset(nodes ...string) {
	r.ring = nil
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes = map[int]string{}
	r.addLocked(nodes...)
}

// Get Get the node corresponding to Key
func (r *HashRing) Get(key string) string {
	// If the hash ring is empty, then it is returned directly
	if r.Empty() {
		return ""
	}

	// Calculate Key hash
	hash := int(r.hash([]byte(key)))

	// Bisect to find the first node that is greater than or equal to the Key hash value
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= hash })

	// This is a special case, where the array has no nodes greater than or equal to the Key hash value
	// But logically this is a ring, so the first node is the target node
	if idx == len(r.ring) {
		idx = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// 返回哈希值对应的真实节点字符串
	return r.nodes[r.ring[idx]]
}
