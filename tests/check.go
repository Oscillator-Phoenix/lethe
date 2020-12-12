package tests

import "bytes"

func isEqualOneBytes(x, y []byte) bool {
	return bytes.Equal(x, y)
}

func isEqualBatchBytes(xs, ys [][]byte) bool {
	if len(xs) != len(ys) {
		return false
	}
	for i := 0; i < len(xs); i++ {
		if isEqualOneBytes(xs[i], ys[i]) == false {
			return false
		}
	}
	return true
}
