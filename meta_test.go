package lethe

import (
	"fmt"
	"testing"
)

func TestKeyMeta(t *testing.T) {
	fmt.Printf("opBase %0*X\n", 16, opBase)
	fmt.Printf("opPut  %0*X\n", 16, opPut)
	fmt.Printf("opDel  %0*X\n", 16, opDel)
}
