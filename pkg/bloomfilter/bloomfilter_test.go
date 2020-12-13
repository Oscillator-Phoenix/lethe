package bloomfilter

import (
	"fmt"
	"testing"
)

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func TestCopy1(t *testing.T) {
	dst := copyBytes(nil)
	fmt.Println("dst", dst, dst == nil)
}

func TestCopy2(t *testing.T) {
	src := []byte{}
	dst := copyBytes(src)
	fmt.Println("src", src)
	fmt.Println("dst", dst, dst == nil)
}
