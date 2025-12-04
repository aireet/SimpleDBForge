package core

import (
	"errors"
	"os"
	"sync"

	"github.com/aireet/SimpleDBForge/lsm/pkg"
	"github.com/aireet/SimpleDBForge/lsm/utils"
)

var errNilFD = errors.New("fd must not be nil")

type WAL struct {
	mu      sync.Mutex
	fd      *os.File
	dir     string
	path    string
	version string
}

func (w *WAL) Write(entries ...pkg.Entry) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fd == nil {
		return 0, errNilFD
	}

	if _, err := w.fd.Seek(0, os.SEEK_END); err != nil {
		return 0, err
	}

	buf := utils.Pool.Get()
	defer utils.Pool.Put(buf)

	for _, entry := range entries {
		cw := utils.Compress(entry.Value)
		if _, err := buf.Write(cw); err != nil {
			return 0, err
		}
	}
	return 0, nil

}
