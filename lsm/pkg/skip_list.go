package pkg

import (
	"math/rand"
	"time"
	"unsafe"

	"github.com/aireet/SimpleDBForge/lsm/utils"
	"github.com/aireet/SimpleDBForge/proto/sdbf"
)

type Element struct {
	*sdbf.Entry
	next []*Element
}

// SkipList
//
// 跳表示例结构（3层）：
// Level 3: HEAD → 3 → 9 → 21 → 26
// Level 2: HEAD → 3 → 6 → 9 → 19 → 21 → 25 → 26
// Level 1: HEAD → 3 → 6 → 7 → 9 → 12 → 19 → 21 → 25 → 26
//
// 节点连接关系：
// - HEAD.next[0] = 3, HEAD.next[1] = 3, HEAD.next[2] = 3
// - 节点3.next[0] = 6, 节点3.next[1] = 6, 节点3.next[2] = 9
// - 节点6.next[0] = 7, 节点6.next[1] = 9
type SkipList struct {
	maxLevel int
	p        float32
	level    int
	rand     *rand.Rand
	size     int
	count    int64
	head     *Element
}

func NewSkipList(maxLevel int, p float64) *SkipList {
	return &SkipList{
		maxLevel: maxLevel,
		p:        float32(p),
		level:    1,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		size:     0,
		head: &Element{
			Entry: &sdbf.Entry{
				Key:       "HEAD",
				Value:     nil,
				Tombstone: false,
				Version:   0,
			},
			next: make([]*Element, maxLevel),
		},
	}
}

// randomLevel 生成跳表节点的随机层级
// 使用概率算法决定节点在跳表中的高度，保证跳表的平衡性
//
// 算法原理：
// - 初始层级为1（每个节点至少出现在第1层）
// - 以概率s.p决定是否提升到更高层级
// - 节点出现在第i层的概率为p^(i-1)，呈几何分布
//
// 参数：
// - s.p: 提升概率，通常取0.25或0.5
// - s.maxLevel: 跳表允许的最大层级
//
// 返回：
// - 节点的随机层级（1到maxLevel之间）
//
// 时间复杂度：O(maxLevel)，但实际很快，因为p通常较小
// 假设 maxLevel = 4 ， p = 0.5 ：

// - 50% 概率： level = 1 （节点只出现在第 1 层）
// - 25% 概率： level = 2 （节点出现在第 1、2 层）
// - 12.5% 概率： level = 3 （节点出现在第 1、2、3 层）
// - 12.5% 概率： level = 4 （节点出现在所有层）
func (s *SkipList) randomLevel() int {
	level := 1
	for level < s.maxLevel && s.rand.Float32() < s.p {
		level++
	}
	return level
}

func (s *SkipList) Reset() *SkipList {
	return NewSkipList(s.maxLevel, float64(s.p))
}

func (s *SkipList) GetSize() int {
	return s.size
}

// Set 在跳表中插入或更新一个条目
//
// 跳表插入过程：
// 1. 从最高层开始，快速找到大致位置
// 2. 逐层下降，精确地定位插入点
// 3. 如果找到了相同的key，就更新数据
// 4. 如果没找到，就在第一层插入新节点，并随机决定这个节点在多少层中可见
//
// 插入示例（插入节点8，随机层级为2）：
// 插入前：
// Level 3: HEAD → 3 → 9 → 21 → 26
// Level 2: HEAD → 3 → 6 → 9 → 19 → 21 → 25 → 26
// Level 1: HEAD → 3 → 6 → 7 → 9 → 12 → 19 → 21 → 25 → 26
//
// 插入后：
// Level 3: HEAD → 3 → 9 → 21 → 26
// Level 2: HEAD → 3 → 6 → 8 → 9 → 19 → 21 → 25 → 26
// Level 1: HEAD → 3 → 6 → 7 → 8 → 9 → 12 → 19 → 21 → 25 → 26
//
// 时间复杂度：O(log n)
func (s *SkipList) Set(entry *sdbf.Entry) {
	// 从顶层开始搜索，记录每层需要更新的前置节点
	curr := s.head
	update := make([]*Element, s.maxLevel)

	// 从最高层往下搜索，记录路径上每层的最后节点
	for i := s.maxLevel - 1; i >= 0; i-- {
		// 在当前层向右移动，直到找到插入位置
		for curr.next[i] != nil && utils.CompareKey(curr.next[i].Key, entry.Key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// 检查key是否已存在，如果存在则更新
	if curr.next[0] != nil && utils.CompareKey(curr.next[0].Key, entry.Key) == 0 {
		// 更新现有条目，调整内存统计
		s.size += len(entry.Value) - len(curr.next[0].Value)
		curr.next[0].Value = entry.Value
		curr.next[0].Tombstone = entry.Tombstone
		return
	}

	// 插入新条目
	// 随机生成节点层级（决定这个节点在几层"立交桥"上可见）
	level := s.randomLevel()

	// 如果生成的层级超过了当前跳表的最大层级，需要扩展
	if level > s.level {
		// 新的层级需要更新所有层的头节点
		for i := s.level; i < level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	// 创建新节点
	e := &Element{
		Entry: entry,
		next:  make([]*Element, level),
	}

	// 在每一层建立连接关系（像在多层立交桥上建立匝道）
	for i := range level {
		e.next[i] = update[i].next[i] // 新节点指向原来的下一个节点
		update[i].next[i] = e         // 前置节点指向新节点
	}

	// 更新内存统计信息
	s.size += len(entry.Key) + len(entry.Value) +
		int(unsafe.Sizeof(entry.Tombstone)) +
		int(unsafe.Sizeof(entry.Version)) +
		len(e.next)*int(unsafe.Sizeof((*Element)(nil)))
	s.count++
}

func (s *SkipList) Get(key string) (*sdbf.Entry, bool) {
	curr := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && utils.CompareKey(curr.next[i].Key, key) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]
	if curr != nil && curr.Key == key {
		return curr.Entry, true
	}
	return nil, false
}

func (s *SkipList) Scan(start, end string) []*sdbf.Entry {
	curr := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && utils.CompareKey(curr.next[i].Key, start) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]
	entries := make([]*sdbf.Entry, 0)
	for curr != nil && utils.CompareKey(curr.Key, end) <= 0 {
		entries = append(entries, curr.Entry)
		curr = curr.next[0]
	}
	return entries
}

func (s *SkipList) All() []*sdbf.Entry {
	all := make([]*sdbf.Entry, s.count)
	index := 0
	for curr := s.head.next[0]; curr != nil; curr = curr.next[0] {
		all[index] = curr.Entry
		index++
	}
	return all
}
