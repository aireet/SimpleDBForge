package utils

import (
	"fmt"
	"testing"
)

// TestCompression 测试压缩功能并输出压缩前后大小
func TestCompression(t *testing.T) {
	// 测试数据：一段重复的文本
	testData := []byte(`这是一段测试数据，用于验证压缩效果。
重复的内容会提高压缩率：这是一段测试数据，用于验证压缩效果。
重复的内容会提高压缩率：这是一段测试数据，用于验证压缩效果。
重复的内容会提高压缩率：这是一段测试数据，用于验证压缩效果。
重复的内容会提高压缩率：这是一段测试数据，用于验证压缩效果。
重复的内容会提高压缩率：这是一段测试数据，用于验证压缩效果。`)

	// 压缩前大小
	originalSize := len(testData)
	fmt.Printf("压缩前大小: %d 字节 (%.2f KB)\n", originalSize, float64(originalSize)/1024)

	// 压缩数据
	compressedData := Compress(testData)
	compressedSize := len(compressedData)
	fmt.Printf("压缩后大小: %d 字节 (%.2f KB)\n", compressedSize, float64(compressedSize)/1024)

	// 计算压缩率
	compressionRatio := float64(compressedSize) / float64(originalSize) * 100
	fmt.Printf("压缩率: %.2f%%\n", compressionRatio)

	// 解压缩验证
	decompressedData, err := Decompress(compressedData)
	if err != nil {
		fmt.Printf("解压缩失败: %v\n", err)
		return
	}

	// 验证数据完整性
	if string(decompressedData) == string(testData) {
		fmt.Println("✓ 压缩解压缩验证成功，数据完整")
	} else {
		fmt.Println("✗ 压缩解压缩验证失败，数据损坏")
	}

	// 输出压缩效果分析
	if compressionRatio < 100 {
		fmt.Printf("✓ 压缩有效，节省了 %.2f%% 的空间\n", 100-compressionRatio)
	} else {
		fmt.Println("⚠ 压缩效果不明显或数据已压缩")
	}
}
