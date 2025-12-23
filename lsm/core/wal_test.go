package core

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aireet/SimpleDBForge/proto/sdbf"
)

// 创建测试用的WAL实例
func createTestWAL(t *testing.T) (*WAL, string) {
	// 创建临时目录
	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "test.wal")

	t.Log("wal path: ", walPath)
	// 打开文件
	fd, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("无法创建WAL文件: %v", err)
		return nil, ""
	}

	wal := &WAL{
		fd:      fd,
		dir:     tmpDir,
		path:    walPath,
		version: "v1.0",
	}

	return wal, walPath
}

// 测试基本的写入和读取功能
func TestWAL_BasicWriteRead(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.fd.Close()

	// 准备测试数据
	testEntries := []*sdbf.Entry{
		{
			Key:       "user:001",
			Value:     []byte("Alice"),
			Tombstone: false,
			Version:   1,
		},
		{
			Key:       "user:002",
			Value:     []byte("Bob"),
			Tombstone: false,
			Version:   2,
		},
		{
			Key:       "user:003",
			Value:     []byte("Charlie"),
			Tombstone: true, // 删除标记
			Version:   3,
		},
	}

	// 测试写入
	t.Run("写入测试", func(t *testing.T) {
		_, err := wal.Write(testEntries...)
		if err != nil {
			t.Fatalf("写入失败: %v", err)
		}

		// 验证文件大小不为0
		stat, err := wal.fd.Stat()
		if err != nil {
			t.Fatalf("无法获取文件信息: %v", err)
		}
		if stat.Size() == 0 {
			t.Fatal("文件大小为0，写入可能失败")
		}

		t.Logf("WAL文件大小: %d 字节", stat.Size())
	})

	// 测试读取
	t.Run("读取测试", func(t *testing.T) {
		entries, err := wal.ReadAll()
		if err != nil {
			t.Fatalf("读取失败: %v", err)
		}

		// 验证读取的条目数量
		if len(entries) != len(testEntries) {
			t.Fatalf("期望读取 %d 条记录，实际读取 %d 条", len(testEntries), len(entries))
		}

		// 验证每条记录的内容
		for i, expected := range testEntries {
			actual := entries[i]

			if actual.Key != expected.Key {
				t.Errorf("记录 %d Key不匹配: 期望 %s, 实际 %s", i, expected.Key, actual.Key)
			}

			if string(actual.Value) != string(expected.Value) {
				t.Errorf("记录 %d Value不匹配: 期望 %s, 实际 %s", i, string(expected.Value), string(actual.Value))
			}

			if actual.Tombstone != expected.Tombstone {
				t.Errorf("记录 %d Tombstone不匹配: 期望 %t, 实际 %t", i, expected.Tombstone, actual.Tombstone)
			}

			if actual.Version != expected.Version {
				t.Errorf("记录 %d Version不匹配: 期望 %d, 实际 %d", i, expected.Version, actual.Version)
			}

			t.Logf("entry: %+v", actual)
		}

		t.Logf("✅ 成功读取并验证 %d 条记录", len(entries))
	})
}

// 测试分批读取功能
func TestWAL_ReadNext(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.fd.Close()

	// 写入10条测试数据
	var testEntries []*sdbf.Entry
	for i := 0; i < 10; i++ {
		entry := &sdbf.Entry{
			Key:       "batch_test:" + string(rune(i+'0')),
			Value:     []byte("测试数据" + string(rune(i+'0'))),
			Tombstone: i%3 == 0, // 每3条设置一次删除标记
			Version:   int64(i + 1),
		}
		testEntries = append(testEntries, entry)
	}

	// 写入所有数据
	_, err := wal.Write(testEntries...)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 将文件指针重置到开头准备读取
	_, err = wal.fd.Seek(0, 0)
	if err != nil {
		t.Fatalf("重置文件指针失败: %v", err)
	}

	// 分批读取测试
	t.Run("分批读取", func(t *testing.T) {
		var allEntries []*sdbf.Entry
		batchSize := 3

		for {
			entries, hasMore, err := wal.readNext(batchSize)
			if err != nil {
				t.Fatalf("分批读取失败: %v", err)
			}

			allEntries = append(allEntries, entries...)
			t.Logf("本批读取 %d 条记录, hasMore: %t", len(entries), hasMore)

			if !hasMore {
				break
			}
		}

		// 验证总数量
		if len(allEntries) != len(testEntries) {
			t.Fatalf("分批读取总数量不匹配: 期望 %d, 实际 %d", len(testEntries), len(allEntries))
		}

		// 验证内容
		for i, expected := range testEntries {
			actual := allEntries[i]
			if actual.Key != expected.Key {
				t.Errorf("分批读取记录 %d Key不匹配: 期望 %s, 实际 %s", i, expected.Key, actual.Key)
			}
		}

		t.Logf("✅ 分批读取验证成功，共读取 %d 条记录", len(allEntries))
	})
}

// 测试大数据写入读取
func TestWAL_LargeData(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.fd.Close()

	// 创建大数据条目 (1MB数据)
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	largeEntry := &sdbf.Entry{
		Key:       "large_data_key",
		Value:     largeValue,
		Tombstone: false,
		Version:   1,
	}

	t.Run("大数据写入", func(t *testing.T) {
		_, err := wal.Write(largeEntry)
		if err != nil {
			t.Fatalf("大数据写入失败: %v", err)
		}

		t.Logf("✅ 成功写入 %d 字节的大数据", len(largeValue))
	})

	t.Run("大数据读取", func(t *testing.T) {
		entries, err := wal.ReadAll()
		if err != nil {
			t.Fatalf("大数据读取失败: %v", err)
		}

		if len(entries) != 1 {
			t.Fatalf("期望读取 1 条记录，实际读取 %d 条", len(entries))
		}

		actual := entries[0]
		if actual.Key != largeEntry.Key {
			t.Errorf("大数据Key不匹配")
		}

		if len(actual.Value) != len(largeEntry.Value) {
			t.Fatalf("大数据Value长度不匹配: 期望 %d, 实际 %d", len(largeEntry.Value), len(actual.Value))
		}

		// 验证数据完整性
		for i := range largeValue {
			if actual.Value[i] != largeValue[i] {
				t.Fatalf("大数据在位置 %d 不匹配", i)
			}
		}

		t.Logf("✅ 大数据读取验证成功，数据完整")
	})
}

// 测试多次追加写入
func TestWAL_MultipleWrites(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.fd.Close()

	var allEntries []*sdbf.Entry

	// 进行3轮写入，每轮写入不同数量的数据
	rounds := []int{2, 3, 5}

	for round, count := range rounds {
		t.Run("第"+string(rune(round+1))+"轮写入", func(t *testing.T) {
			var roundEntries []*sdbf.Entry

			for i := 0; i < count; i++ {
				entry := &sdbf.Entry{
					Key:       "round_" + string(rune(round+'0')) + "_item_" + string(rune(i+'0')),
					Value:     []byte("round " + string(rune(round+'0')) + " item " + string(rune(i+'0'))),
					Tombstone: false,
					Version:   int64(round*10 + i),
				}
				roundEntries = append(roundEntries, entry)
			}

			_, err := wal.Write(roundEntries...)
			if err != nil {
				t.Fatalf("第 %d 轮写入失败: %v", round+1, err)
			}

			allEntries = append(allEntries, roundEntries...)
			t.Logf("第 %d 轮成功写入 %d 条记录", round+1, count)
		})
	}

	// 验证最终读取结果
	t.Run("验证所有写入", func(t *testing.T) {
		entries, err := wal.ReadAll()
		if err != nil {
			t.Fatalf("读取所有记录失败: %v", err)
		}

		if len(entries) != len(allEntries) {
			t.Fatalf("总记录数不匹配: 期望 %d, 实际 %d", len(allEntries), len(entries))
		}

		for i, expected := range allEntries {
			actual := entries[i]
			if actual.Key != expected.Key || string(actual.Value) != string(expected.Value) {
				t.Errorf("记录 %d 不匹配: 期望 key=%s value=%s, 实际 key=%s value=%s",
					i, expected.Key, string(expected.Value), actual.Key, string(actual.Value))
			}
		}

		t.Logf("✅ 多轮写入验证成功，共 %d 条记录", len(entries))
	})
}

// 测试错误处理
func TestWAL_ErrorHandling(t *testing.T) {
	wal, _ := createTestWAL(t)

	// 测试关闭文件后的操作
	t.Run("已关闭文件的错误处理", func(t *testing.T) {
		wal.fd.Close()

		entry := &sdbf.Entry{Key: "test", Value: []byte("value")}

		// 写入应该返回错误
		_, err := wal.Write(entry)
		if err != errNilFD {
			t.Errorf("期望错误 %v, 实际错误 %v", errNilFD, err)
		}

		// 读取也应该返回错误
		_, err = wal.ReadAll()
		if err != errNilFD {
			t.Errorf("期望错误 %v, 实际错误 %v", errNilFD, err)
		}

		t.Log("✅ 错误处理测试通过")
	})
}

// 性能基准测试
func BenchmarkWAL_Write(b *testing.B) {
	// 创建临时文件
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench.wal")
	fd, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		b.Fatalf("无法创建WAL文件: %v", err)
	}
	defer fd.Close()

	wal := &WAL{fd: fd}

	entry := &sdbf.Entry{
		Key:     "benchmark_key",
		Value:   []byte("benchmark_value_with_some_content"),
		Version: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(entry)
	}
}

func BenchmarkWAL_Read(b *testing.B) {
	// 准备测试数据
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench_read.wal")
	fd, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		b.Fatalf("无法创建WAL文件: %v", err)
	}
	defer fd.Close()

	wal := &WAL{fd: fd}

	// 预先写入一些数据
	for i := 0; i < 1000; i++ {
		entry := &sdbf.Entry{
			Key:     "key_" + string(rune(i)),
			Value:   []byte("value_" + string(rune(i))),
			Version: int64(i),
		}
		wal.Write(entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.ReadAll()
	}
}
