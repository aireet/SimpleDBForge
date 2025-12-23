package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/aireet/SimpleDBForge/api/sdbf"
	"github.com/aireet/SimpleDBForge/internal/utils"
)

var (
	errNilFD            = errors.New("fd must not be nil")
	errInvalidEntrySize = errors.New("invalid entry size")
	errCorruptedWAL     = errors.New("WAL file is corrupted")
)

type WAL struct {
	mu      sync.Mutex
	fd      *os.File
	dir     string
	path    string
	version string
}

func NewWAL(fd *os.File, dir, path, version string) *WAL {
	return &WAL{
		fd:      fd,
		dir:     dir,
		path:    path,
		version: version,
	}
}

func (w *WAL) Write(entries ...*sdbf.Entry) (int, error) {

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fd == nil {
		return 0, errNilFD
	}
	// 将文件指针移动到文件末尾, 用于实现 WAL 追加
	if _, err := w.fd.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}

	buf := utils.Pool.Get()
	defer utils.Pool.Put(buf)

	count := 0
	for _, entry := range entries {

		// [数据长度] + [数据内容] 小端序
		// ## 为什么选择小端序
		// 1. 兼容性好 ：x86/x64 架构（最常见的服务器架构）使用小端序
		// 2. 性能优势 ：在小端序机器上无需字节序转换
		// 3. 标准选择 ：许多网络协议和文件格式采用小端序
		data, err := proto.Marshal(entry)
		if err != nil {
			return count, err
		}
		// 写入数据长度（8字节）
		if err := binary.Write(buf, binary.LittleEndian, int64(len(data))); err != nil {
			return count, fmt.Errorf("failed to write data length: %w", err)
		}
		// 写入实际数据内容
		if _, err := buf.Write(data); err != nil {
			return count, fmt.Errorf("failed to write data: %w", err)
		}
		count++
	}

	// 写入磁盘
	if _, err := buf.WriteTo(w.fd); err != nil {
		return count, err
	}
	if err := w.fd.Sync(); err != nil {
		return count, err
	}
	return count, nil
}

func (w *WAL) ReadAll() ([]*sdbf.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fd == nil {
		return nil, errNilFD
	}

	// 将文件指针移动到文件开头
	if _, err := w.fd.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var Allentries []*sdbf.Entry

	batchSize := 1000
	for {
		entries, hasMore, err := w.readNext(batchSize)
		if err != nil {
			return nil, err
		}
		Allentries = append(Allentries, entries...)
		if !hasMore {
			break
		}
	}

	return Allentries, nil
}

func (w *WAL) ReadBatch(batchSize int) (chan []*sdbf.Entry, error) {

	if w.fd == nil {
		return nil, errNilFD
	}

	entryChan := make(chan []*sdbf.Entry)

	go func() {

		w.mu.Lock()
		defer w.mu.Unlock()

		// 将文件指针移动到文件开头
		if _, err := w.fd.Seek(0, io.SeekStart); err != nil {
			panic(err)
		}

		for {

			entries, hasMore, err := w.readNext(batchSize)
			if err != nil {
				err = fmt.Errorf("read wal failed: %w", err)
				panic(err)
			}

			entryChan <- entries
			if !hasMore {
				close(entryChan)
				break
			}

		}

	}()

	return entryChan, nil
}

// readNext 连续读取指定数量的记录，不重置文件指针
func (w *WAL) readNext(maxCount int) ([]*sdbf.Entry, bool, error) {
	if w.fd == nil {
		return nil, false, errNilFD
	}

	var entries []*sdbf.Entry
	buf := utils.Pool.Get()
	defer utils.Pool.Put(buf)

	for i := 0; i < maxCount; i++ {
		// 读取数据长度
		var dataLen int64
		// 这里 binary.Read 消耗了文件指针的前8个字节 ，读取完后文件指针已经移动到第9个字节的位置。
		// 位置:  [0-7]     [8-242]
		// 内容:  [235]  [JSON数据...]
		err := binary.Read(w.fd, binary.LittleEndian, &dataLen)
		if err == io.EOF {
			return entries, false, nil // 到达文件末尾，hasMore = false
		}
		if err != nil {
			return nil, false, fmt.Errorf("failed to read entry length: %w", err)
		}

		// 验证数据长度的合理性
		if dataLen <= 0 {
			return nil, false, fmt.Errorf("%w: non-positive length %d", errInvalidEntrySize, dataLen)
		}

		// 准备buffer用于读取数据
		buf.Reset()
		if buf.Cap() < int(dataLen) {
			buf.Grow(int(dataLen))
		}

		// 直接从文件读取到buffer中
		n, err := io.CopyN(buf, w.fd, dataLen)
		if err != nil {
			return nil, false, fmt.Errorf("failed to read entry data: %w", err)
		}
		if n != dataLen {
			return nil, false, fmt.Errorf("%w: incomplete entry data, expected %d bytes, got %d", errCorruptedWAL, dataLen, n)
		}
		data := buf.Bytes()

		// 反序列化数据
		e := &sdbf.Entry{}
		if err := proto.Unmarshal(data, e); err != nil {
			return nil, false, fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		entries = append(entries, e)
	}

	return entries, true, nil // 读满指定数量，hasMore = true
}
