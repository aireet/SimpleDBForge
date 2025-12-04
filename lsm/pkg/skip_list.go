package pkg

import (
	"math/rand"
	"time"
	"unsafe"

	"github.com/aireet/SimpleDBForge/lsm/utils"
)

type Element struct {
	Entry
	next []*Element
}

// SkipList
//
// Level 3:       3 ----------- 9 ----------- 21 --------- 26
// Level 2:       3 ----- 6 ---- 9 ------ 19 -- 21 ---- 25 -- 26
// Level 1:       3 -- 6 -- 7 -- 9 -- 12 -- 19 -- 21 -- 25 -- 26
//
// next of head [ ->3, ->3, ->3 ]
// next of Element 3 [ ->6, ->6, ->9 ]
// next of Element 6 [ ->7, ->9 ]
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
			Entry: Entry{
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

func (s *SkipList) Set(entry Entry) {
	curr := s.head
	update := make([]*Element, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && utils.CompareKey(curr.next[i].Key, entry.Key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// update key
	if curr.next[0] != nil && utils.CompareKey(curr.next[0].Key, entry.Key) == 0 {
		s.size += len(entry.Value) - len(curr.next[0].Value)
		curr.next[0].Value = entry.Value
		curr.next[0].Tombstone = entry.Tombstone
		return
	}

	// add entry
	level := s.randomLevel()
	if level > s.level {
		// 新的层级需要更新所有层的头节点
		for i := s.level; i < level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	e := &Element{
		Entry: entry,
		next:  make([]*Element, level),
	}
	for i := range level {
		e.next[i] = update[i].next[i]
		update[i].next[i] = e
	}

	s.size += len(entry.Key) + len(entry.Value) +
		int(unsafe.Sizeof(entry.Tombstone)) +
		int(unsafe.Sizeof(entry.Version)) +
		len(e.next)*int(unsafe.Sizeof((*Element)(nil)))
	s.count++
}

func (s *SkipList) Get(key string) (Entry, bool) {
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
	return Entry{}, false
}

func (s *SkipList) Scan(start, end string) []Entry {
	curr := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && utils.CompareKey(curr.next[i].Key, start) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]
	entries := make([]Entry, 0)
	for curr != nil && utils.CompareKey(curr.Key, end) <= 0 {
		entries = append(entries, curr.Entry)
		curr = curr.next[0]
	}
	return entries
}

func (s *SkipList) All() []Entry {
	all := make([]Entry, s.count)
	index := 0
	for curr := s.head.next[0]; curr != nil; curr = curr.next[0] {
		all[index] = curr.Entry
		index++
	}
	return all
}
