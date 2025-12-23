package utils

import (
	"strconv"
	"strings"
)

func CompareKey(a, b string) int {
	// 提取 key 前缀（去掉 @timestamp 部分）
	aPrefix, aTs := splitKey(a)
	bPrefix, bTs := splitKey(b)

	// 先比较前缀
	if cmp := strings.Compare(aPrefix, bPrefix); cmp != 0 {
		return cmp
	}

	// 前缀相同，按时间戳降序排列（新的在前）
	if aTs < bTs {
		return 1  // a 的时间戳更小，a 应该排在后面
	}
	if aTs > bTs {
		return -1 // a 的时间戳更大，a 应该排在前面
	}
	return 0
}

// splitKey 分割 key 为前缀和时间戳
func splitKey(key string) (prefix string, ts uint64) {
	idx := strings.LastIndex(key, "@")
	if idx == -1 {
		return key, 0
	}
	ts = ParseTs(key)
	if ts == 0 {
		// 解析失败，说明不是有效的 timestamp 格式
		return key, 0
	}
	return key[:idx], ts
}

func ParseTs(key string) uint64 {
	if key == "" {
		return 0
	}
	ts, err := strconv.ParseUint(key[strings.LastIndex(key, "@")+1:], 10, 64)
	if err != nil {
		return 0
	}
	return ts
}
