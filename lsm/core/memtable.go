package core

import (
	"log/slog"
	"sync"

	"github.com/aireet/SimpleDBForge/lsm/pkg"
	"github.com/aireet/SimpleDBForge/proto/sdbf"
)

type MemTable struct {
	sync.Once
	mu       sync.RWMutex
	skipList *pkg.SkipList
	wal      *WAL
	walDir   string
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
	_, err := mt.wal.Write(entry)
	if err != nil {
		return err
	}
	mt.skipList.Set(entry)
	return nil
}

func (mt *MemTable) Get(key string) (*sdbf.Entry, bool) {
	return mt.skipList.Get(key)
}
