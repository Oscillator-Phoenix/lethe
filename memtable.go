package lethe

import "io"

type sortedMemMap interface {
	Size() int
	Empty() bool
	Get(key []byte) (value []byte, ok bool)
	Put(key, value []byte) error
	Del(key []byte) error
	Traverse(operate func(key, value []byte))
}

type memTable struct {
	smm       sortedMemMap
	limitSize int
}

func (mt *memTable) flush(w io.Writer) error {
	return nil
}
