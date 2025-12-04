package utils

import (
	"strconv"
	"strings"
)

func CompareKey(a, b string) int {
	cmp := strings.Compare(a, b)
	if cmp != 0 {
		return cmp
	}
	// sameKey compare timestamp
	// key user:123@1640995200
	tsa := ParseTs(a)
	tsb := ParseTs(b)
	if tsa < tsb {
		return 1
	}
	if tsa > tsb {
		return -1
	}
	return 0
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
