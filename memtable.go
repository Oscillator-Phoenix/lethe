package lethe

import (
	"io"
	"sync"
)

type sortedMapEntity struct {
	key       []byte
	value     []byte
	deleteKey []byte
	meta      keyMeta
}

type sortedMap interface {
	Size() int
	Empty() bool
	Get(key []byte) (entity *sortedMapEntity, ok bool)
	Put(key []byte, entity *sortedMapEntity) error
	Traverse(operation func(key []byte, entity *sortedMapEntity))
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func copySortedMapEntity(src *sortedMapEntity) *sortedMapEntity {
	if src == nil {
		return nil
	}
	dst := &sortedMapEntity{}
	dst.key = copyBytes(src.key)
	dst.value = copyBytes(src.value)
	dst.deleteKey = copyBytes(src.deleteKey)
	dst.meta = src.meta
	return dst
}

type memTable struct {
	mu      sync.Mutex
	smm     sortedMap
	_nBytes int
}

func newMemTable(less func(s, t []byte) bool) *memTable {
	mt := &memTable{}

	mt.smm = newSkipList(less)
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

	entity, found := mt.smm.Get(key)
	if !found {
		return nil, false
	}
	if entity.meta.opType == constOpDel {
		return nil, false
	}
	return copyBytes(entity.value), true
}

// Put inserts a kv entry into memTable.
func (mt *memTable) Put(key, value, deleteKey []byte, meta keyMeta) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt._nBytes += (len(key) + len(value) + len(deleteKey) + 16) // meta is 16 bytes.

	return mt.smm.Put(key, &sortedMapEntity{
		value:     value,
		deleteKey: deleteKey,
		meta:      meta,
	})
}

// Traverse traverses the memTable in order defined by lessFunc
func (mt *memTable) Traverse(operation func(key []byte, entity *sortedMapEntity)) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.smm.Traverse(operation)
}

// flush to w
func (mt *memTable) flush(w io.Writer) error {

	mt.Traverse(func(key []byte, entity *sortedMapEntity) {
		// TODO
	})

	return nil
}
