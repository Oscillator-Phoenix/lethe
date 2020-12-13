package tests

import (
	"fmt"
	"testing"
)

func TestGenOneBytes(t *testing.T) {

	bytesLen := constAvgKeyLen
	bytes := genOneBytes(bytesLen)

	if len(bytes) != bytesLen {
		t.Fail()
	}

	fmt.Println(bytes)
	fmt.Printf("%s\n", string(bytes))
}

func TestGenBatchBytes(t *testing.T) {
	batchSize := 10
	avgBytesLen := 20
	batchBytes := genBatchBytes(batchSize, avgBytesLen)

	if len(batchBytes) != batchSize {
		t.Fail()
	}

	for _, bytes := range batchBytes {
		fmt.Printf("%s %v\n", string(bytes), bytes)
	}
}

func TestGenBatchKVA(t *testing.T) {
	batchSize := 10
	ks, vs, as := genBatchKVA(batchSize)

	if len(ks) != batchSize || len(vs) != batchSize {
		t.Fail()
	}

	for i := 0; i < len(ks); i++ {
		fmt.Printf("{ Key: %s, Value: %s, Final-Value: %s}\n", string(ks[i]), string(vs[i]), string(as[i]))
	}
}
