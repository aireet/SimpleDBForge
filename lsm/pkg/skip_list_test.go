package pkg

import (
	"testing"

	"github.com/aireet/SimpleDBForge/lsm/utils"
)

func TestNewSkipList(t *testing.T) {
	sl := NewSkipList(4, 0.5)
	if sl == nil {
		t.Error("Failed to create SkipList")
	}
	if sl.maxLevel != 4 {
		t.Errorf("Expected maxLevel 4, got %d", sl.maxLevel)
	}
	if sl.level != 1 {
		t.Errorf("Expected initial level 1, got %d", sl.level)
	}
	if sl.count != 0 {
		t.Errorf("Expected initial count 0, got %d", sl.count)
	}
	if sl.size != 0 {
		t.Errorf("Expected initial size 0, got %d", sl.size)
	}
}

func TestSetAndGet(t *testing.T) {
	sl := NewSkipList(4, 0.5)

	// 测试插入和获取
	entry1 := Entry{
		Key:       "user:1",
		Value:     []byte("Alice"),
		Tombstone: false,
		Version:   1,
	}

	sl.Set(entry1)

	// 测试获取存在的key
	result, found := sl.Get("user:1")
	if !found {
		t.Error("Expected to find key 'user:1'")
	}
	if string(result.Value) != "Alice" {
		t.Errorf("Expected value 'Alice', got '%s'", string(result.Value))
	}

	// 测试获取不存在的key
	_, found = sl.Get("user:2")
	if found {
		t.Error("Expected not to find key 'user:2'")
	}
}

func TestUpdateExistingKey(t *testing.T) {
	sl := NewSkipList(4, 0.5)

	// 插入初始值
	entry1 := Entry{
		Key:       "user:1",
		Value:     []byte("Alice"),
		Tombstone: false,
		Version:   1,
	}
	sl.Set(entry1)

	// 更新值
	entry2 := Entry{
		Key:       "user:1",
		Value:     []byte("Bob"),
		Tombstone: false,
		Version:   2,
	}
	sl.Set(entry2)

	// 验证更新成功
	result, found := sl.Get("user:1")
	if !found {
		t.Error("Expected to find key 'user:1'")
	}
	if string(result.Value) != "Bob" {
		t.Errorf("Expected updated value 'Bob', got '%s'", string(result.Value))
	}

	// 验证count没有增加
	if sl.count != 1 {
		t.Errorf("Expected count 1 after update, got %d", sl.count)
	}
}

func TestAll(t *testing.T) {
	sl := NewSkipList(4, 0.5)

	// 插入多个条目
	entries := []Entry{
		{Key: "user:3", Value: []byte("Charlie")},
		{Key: "user:1", Value: []byte("Alice")},
		{Key: "user:2", Value: []byte("Bob")},
	}

	for _, entry := range entries {
		sl.Set(entry)
	}

	// 获取所有条目
	all := sl.All()
	if len(all) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(all))
	}

	// 验证排序正确（应该是按key排序）
	expectedOrder := []string{"user:1", "user:2", "user:3"}
	for i, key := range expectedOrder {
		if all[i].Key != key {
			t.Errorf("Expected key '%s' at position %d, got '%s'", key, i, all[i].Key)
		}
	}
}

func TestScan(t *testing.T) {
	sl := NewSkipList(4, 0.5)

	// 插入多个条目
	entries := []Entry{
		{Key: "a", Value: []byte("first")},
		{Key: "b", Value: []byte("second")},
		{Key: "c", Value: []byte("third")},
		{Key: "d", Value: []byte("fourth")},
		{Key: "e", Value: []byte("fifth")},
	}

	for _, entry := range entries {
		sl.Set(entry)
	}

	// 扫描范围
	result := sl.Scan("b", "d")
	if len(result) != 3 {
		t.Errorf("Expected 3 entries in scan, got %d", len(result))
	}

	expectedKeys := []string{"b", "c", "d"}
	for i, entry := range result {
		if entry.Key != expectedKeys[i] {
			t.Errorf("Expected key '%s' at position %d, got '%s'", expectedKeys[i], i, entry.Key)
		}
	}
}

func TestRandomLevel(t *testing.T) {
	sl := NewSkipList(10, 0.5)

	// 测试randomLevel生成的层级在合理范围内
	for i := 0; i < 100; i++ {
		level := sl.randomLevel()
		if level < 1 || level > sl.maxLevel {
			t.Errorf("Random level %d out of range [1, %d]", level, sl.maxLevel)
		}
	}
}

func TestParseTs(t *testing.T) {
	// 测试时间戳解析
	keyWithTs := "user:123@1640995200"
	ts := utils.ParseTs(keyWithTs)
	if ts != 1640995200 {
		t.Errorf("Expected timestamp 1640995200, got %d", ts)
	}

	// 测试空字符串
	emptyTs := utils.ParseTs("")
	if emptyTs != 0 {
		t.Errorf("Expected 0 for empty string, got %d", emptyTs)
	}

	// 测试没有@符号的字符串
	noAtTs := utils.ParseTs("user:123")
	if noAtTs != 0 {
		t.Errorf("Expected 0 for string without @, got %d", noAtTs)
	}
}

func TestCompareKey(t *testing.T) {
	// 测试相同key不同时间戳的比较
	key1 := "user:123@100"
	key2 := "user:123@200"

	// 时间戳大的应该排在前面（返回负值）
	result := utils.CompareKey(key1, key2)
	if result <= 0 {
		t.Errorf("Expected key2 to be greater (negative result), got %d", result)
	}

	// 时间戳小的应该排在后面（返回正值）
	result = utils.CompareKey(key2, key1)
	if result >= 0 {
		t.Errorf("Expected key1 to be greater (positive result), got %d", result)
	}

	// 测试不同key的比较
	diffKey1 := "user:122@100"
	diffKey2 := "user:123@100"
	result = utils.CompareKey(diffKey1, diffKey2)
	if result >= 0 {
		t.Errorf("Expected diffKey1 to be less than diffKey2, got %d", result)
	}
}

func TestMemoryTracking(t *testing.T) {
	sl := NewSkipList(4, 0.5)
	initialSize := sl.GetSize()
	initialCount := sl.count

	// 插入条目
	entry := Entry{
		Key:   "test_key",
		Value: []byte("test_value"),
	}
	sl.Set(entry)

	// 验证计数器增加
	if sl.count != initialCount+1 {
		t.Errorf("Expected count to increase by 1, got %d", sl.count-initialCount)
	}

	// 验证大小增加
	if sl.GetSize() <= initialSize {
		t.Error("Expected size to increase after insertion")
	}

	// 更新条目（不改变count）
	entryUpdated := Entry{
		Key:   "test_key",
		Value: []byte("updated_test_value"),
	}
	sl.Set(entryUpdated)

	if sl.count != initialCount+1 {
		t.Errorf("Expected count to remain the same after update, got %d", sl.count)
	}
}

func TestReset(t *testing.T) {
	sl := NewSkipList(4, 0.5)

	// 插入一些数据
	entry := Entry{Key: "test", Value: []byte("value")}
	sl.Set(entry)

	if sl.count != 1 {
		t.Errorf("Expected count 1 before reset, got %d", sl.count)
	}

	// 重置
	sl = sl.Reset()

	if sl.count != 0 {
		t.Errorf("Expected count 0 after reset, got %d", sl.count)
	}
	if sl.level != 1 {
		t.Errorf("Expected level 1 after reset, got %d", sl.level)
	}
}

func BenchmarkSkipListSet(b *testing.B) {
	sl := NewSkipList(4, 0.5)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entry := Entry{
			Key:   "key" + string(rune(i)),
			Value: []byte("value" + string(rune(i))),
		}
		sl.Set(entry)
	}
}

func BenchmarkSkipListGet(b *testing.B) {
	sl := NewSkipList(4, 0.5)

	// 预先插入数据
	for i := 0; i < 1000; i++ {
		entry := Entry{
			Key:   "key" + string(rune(i)),
			Value: []byte("value" + string(rune(i))),
		}
		sl.Set(entry)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "key" + string(rune(i%1000))
		sl.Get(key)
	}
}

func TestConcurrentAccess(t *testing.T) {
	// 注意：当前实现不是线程安全的
	// 这个测试主要是为了文档化当前的行为
	sl := NewSkipList(4, 0.5)

	// 快速连续操作
	for i := 0; i < 100; i++ {
		entry := Entry{
			Key:   "key" + string(rune(i)),
			Value: []byte("value" + string(rune(i))),
		}
		sl.Set(entry)
	}

	// 验证所有数据都能正确获取
	for i := 0; i < 100; i++ {
		key := "key" + string(rune(i))
		_, found := sl.Get(key)
		if !found {
			t.Errorf("Expected to find key %s", key)
		}
	}
}

func TestEdgeCases(t *testing.T) {
	sl := NewSkipList(1, 0.5) // 最小层级

	// 测试空key
	entry := Entry{
		Key:   "",
		Value: []byte("empty_key_value"),
	}
	sl.Set(entry)

	result, found := sl.Get("")
	if !found {
		t.Error("Expected to find empty key")
	}
	if string(result.Value) != "empty_key_value" {
		t.Errorf("Expected 'empty_key_value', got '%s'", string(result.Value))
	}

	// 测试特殊字符key
	specialKey := "!@#$%^&*()"
	entry2 := Entry{
		Key:   specialKey,
		Value: []byte("special_value"),
	}
	sl.Set(entry2)

	_, found = sl.Get(specialKey)
	if !found {
		t.Error("Expected to find special key")
	}
}
