package lethe

import (
	"io"
	"lethe/skiplist"
	"sync"
)

type sortedMemMap interface {
	Size() int
	Empty() bool
	Get(key []byte) (value []byte, ok bool)
	Put(key, value []byte) error
	Del(key []byte) error
	Traverse(operate func(key, value []byte))
}

type memTable struct {
	mu      sync.Mutex
	smm     sortedMemMap
	_nBytes int
}

func newMemTable(less func(s, t []byte) bool) *memTable {
	mt := &memTable{}

	mt.smm = skiplist.NewSkipList(less)
	mt._nBytes = 0

	return mt
}

// nBytes return the number of bytes used
func (mt *memTable) nBytes() int {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt._nBytes
}

// Size returns the number of kv entries in the memTable
func (mt *memTable) Size() int {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.smm.Size()
}

// Empty returns whether the memTable is empty
func (mt *memTable) Empty() bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.smm.Empty()
}

// Get returns the copy of value by key.
// If the key is not found, it returns (nil, false).
func (mt *memTable) Get(key []byte) (value []byte, ok bool) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.smm.Get(key)
}

// Put inserts a kv entry into memTable.
func (mt *memTable) Put(key, value []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt._nBytes += (len(key) + len(value))

	return mt.smm.Put(key, value)
}

// Del the kv entry by key
func (mt *memTable) Del(key []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	err := mt.smm.Del(key)
	if err == nil {
		mt._nBytes -= (len(key))
	}

	return err
}

// Traverse traverses the memTable in order defined by lessFunc
func (mt *memTable) Traverse(operate func(key, value []byte)) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.smm.Traverse(operate)
}

// flush to w
func (mt *memTable) flush(w io.Writer) error {

	// TODO
	mt.Traverse(func(key, value []byte) {

	})

	return nil
}
