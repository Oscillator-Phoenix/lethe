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
	// str := fmt.Sprintf("%0*v", bytesLen, rand.Int())
	str := fmt.Sprintf("%0*v", bytesLen, rand.Intn(1<<20))

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

func genFinalValue(ks, vs [][]byte) [][]byte {
	m := map[string]string{}
	as := make([][]byte, len(vs))

	for i := 0; i < len(ks); i++ {
		m[string(ks[i])] = string(vs[i])
	}
	for i := 0; i < len(ks); i++ {
		as[i] = []byte(m[string(ks[i])])
	}

	return as
}

// genBatchKVA generate (key, value, final-value)
func genBatchKVA(batchSize int) ([][]byte, [][]byte, [][]byte) {
	ks := genBatchBytes(batchSize, constAvgKeyLen)
	vs := genBatchBytes(batchSize, constAvgValueLen)
	as := genFinalValue(ks, vs)

	return ks, vs, as
}
