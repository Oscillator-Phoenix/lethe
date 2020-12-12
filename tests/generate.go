package tests

import (
	"fmt"
	"math/rand"
	"time"
)

// generate different workload of KV datavase

const (
	_B  = 1
	_KB = (_B << 10)
	_MB = (_KB << 10)
	_GB = (_MB << 10)

	constAvgKeyLen   = 5 * _B
	constAvgValueLen = 20 * _B
)

var (
	globalRandSeed int64 = func() int64 {
		seed := time.Now().UnixNano()
		rand.Seed(seed)
		return seed
	}()
)

func newRandSeed() {
	globalRandSeed := time.Now().UnixNano()
	rand.Seed(globalRandSeed)
}

func genOneBytes(bytesLen int) []byte {
	str := fmt.Sprintf("%0*v", bytesLen, rand.Int())
	return []byte(str)[:bytesLen]
}

func genBatchBytes(batchSize int, avgBytesLen int) [][]byte {
	keys := make([][]byte, batchSize)

	for i := 0; i < batchSize; i++ {
		bytesLen := 1 + rand.Intn(avgBytesLen*2)
		keys[i] = genOneBytes(bytesLen)
	}

	return keys
}

func genBatchKV(batchSize int) ([][]byte, [][]byte) {
	ks := genBatchBytes(batchSize, constAvgKeyLen)
	vs := genBatchBytes(batchSize, constAvgValueLen)
	return ks, vs
}
