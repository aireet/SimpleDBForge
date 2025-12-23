package lsm

import (
	"log/slog"
	"sync"

	"github.com/aireet/SimpleDBForge/api/sdbf"
	"github.com/aireet/SimpleDBForge/pkg/skiplist"
)

type MemTable struct {
	sync.Once
	mu       sync.RWMutex
	skipList *skiplist.SkipList
	wal      *WAL
	walDir   string
}

func NewMebTable(walDir string) *MemTable {
	return &MemTable{
		skipList: skiplist.NewSkipList(4, 0.5),
		walDir:   walDir,
	}
}

func (mt *MemTable) Recovery() {

	mt.Once.Do(func() {

		// 从wal log 中重放数据到 skip list
		entryChan, err := mt.wal.ReadBatch(1000)
		if err != nil {
			slog.Error("recovery memtable", "err", err)
			return
		}

		for {
			entries := <-entryChan
			if entries == nil {
				break
			}
			for _, entry := range entries {
				mt.skipList.Set(entry)
			}
		}

	})
}

func (mt *MemTable) Set(entry *sdbf.Entry) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	_, err := mt.wal.Write(entry)
	if err != nil {
		return err
	}
	mt.skipList.Set(entry)
	return nil
}

func (mt *MemTable) Get(key string) (*sdbf.Entry, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.skipList.Get(key)
}
