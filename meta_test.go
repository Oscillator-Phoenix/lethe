package lethe

import (
	"fmt"
	"testing"
)

func TestKeyMeta(t *testing.T) {
	fmt.Printf("constOpBase %0*X\n", 16, constOpBase)
	fmt.Printf("constOpPut  %0*X\n", 16, constOpPut)
	fmt.Printf("constOpDel  %0*X\n", 16, constOpDel)
}
