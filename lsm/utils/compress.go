package utils

import (
	"runtime"

	"github.com/klauspost/compress/zstd"
)

var (
	smallEecoder *zstd.Encoder
	smallDecoder *zstd.Decoder
)

func init() {
	numCpu := runtime.GOMAXPROCS(0)
	smallEecoder, _ = zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderConcurrency(numCpu),
	)
	smallDecoder, _ = zstd.NewReader(
		nil,
		zstd.WithDecoderConcurrency(numCpu),
	)
}

func Compress(src []byte) []byte {
	return smallEecoder.EncodeAll(src, nil)
}

func Decompress(src []byte) ([]byte, error) {
	return smallDecoder.DecodeAll(src, nil)
}
