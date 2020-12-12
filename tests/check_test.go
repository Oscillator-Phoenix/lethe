package tests

import "testing"

func TestIsEqualBatchBytes(t *testing.T) {
	batchSize := 100
	avgBytesLen := 1000
	bs := genBatchBytes(batchSize, avgBytesLen)
	if isEqualBatchBytes(bs, bs) == false {
		t.Fail()
	}
}
