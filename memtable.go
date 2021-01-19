package lethe

import (
	"fmt"
	"log"
	"sync"
)

type sortedMapEntity struct {
	value     []byte
	deleteKey []byte
	meta      keyMeta
}

// A sortedMap is a sorted map mapping key to sortedMapEntity.
type sortedMap interface {
	Num() int
	Empty() bool
	Get(key []byte) (entity *sortedMapEntity, ok bool)
	Put(key []byte, entity *sortedMapEntity) error
	Traverse(operation func(key []byte, entity *sortedMapEntity))
}

type memTable struct {
	sync.Mutex

	nBytes int
	less   func(s, t []byte) bool
	sm     sortedMap
}

type immutableMemTable struct {
	memTable
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
	dst.value = copyBytes(src.value)
	dst.deleteKey = copyBytes(src.deleteKey)
	dst.meta = src.meta
	return dst
}

func (e sortedMapEntity) String() string {

	opTypePrint := func(opType uint64) string {
		switch e.meta.opType {
		case opPut:
			return "Put"
		case opDel:
			return "Del"
		default:
			return "Unknown"
		}
	}

	return fmt.Sprintf("value [%s], deleteKey [%s], meta.seqNum [%d], meta.opType [%s]",
		string(e.value),
		string(e.deleteKey),
		e.meta.seqNum,
		opTypePrint(e.meta.opType))
}

// ---------------------------------------------------------------------------

func newMemTable(less func(s, t []byte) bool) *memTable {
	mt := &memTable{}

	mt.nBytes = 0
	mt.less = less
	mt.sm = newSkipList(mt.less)

	return mt
}

// ---------------------------------------------------------------------------

// thread-safe
func (mt *memTable) resetIfNecessary(memTableSizeLimit int) (reset bool, imt *immutableMemTable) {

	// a trick: test and lock

	if mt.nBytes < memTableSizeLimit { // test without locking
		return false, nil
	}

	mt.Lock()         // lock
	defer mt.Unlock() // unlock

	if mt.nBytes < memTableSizeLimit { // test with locking
		return false, nil
	}

	// immute
	imt = &immutableMemTable{}

	// just give the ownership of sortedMap to the new immutableMemTable
	imt.nBytes = mt.nBytes
	imt.less = mt.less
	imt.sm = mt.sm

	// reset this memTable
	mt.nBytes = 0
	mt.sm = newSkipList(mt.less)

	log.Printf("reset current memTable [%d] -> [%d]\n", imt.nBytes, mt.nBytes)

	return true, imt
}

// ---------------------------------------------------------------------------

// Num returns the number of kv entries in the memTable
// thread-safe
func (mt *memTable) Num() int {
	mt.Lock()
	defer mt.Unlock()

	return mt.sm.Num()
}

// Empty returns whether the memTable is empty
// thread-safe
func (mt *memTable) Empty() bool {
	mt.Lock()
	defer mt.Unlock()

	return mt.sm.Empty()
}

// Get returns the copy of value by key.
// If the key is not found, it returns (false, nil, meta).
// thread-safe
func (mt *memTable) Get(key []byte) (found bool, value []byte, meta keyMeta) {
	mt.Lock()
	defer mt.Unlock()

	entity, found := mt.sm.Get(key)

	// key is not found
	if !found {
		return false, nil, meta
	}

	// found the valid entity
	return true, copyBytes(entity.value), entity.meta
}

// Put inserts a kv entry into memTable.
// thread-safe
func (mt *memTable) Put(key, value, deleteKey []byte, meta keyMeta) error {
	mt.Lock()
	defer mt.Unlock()

	mt.nBytes += (len(key) + len(value) + len(deleteKey) + constKeyMetaBytesLen)

	entity := &sortedMapEntity{
		value:     value,
		deleteKey: deleteKey,
		meta:      meta,
	}

	// log.Printf("memTable Put { key[%s] : %v }\n", string(key), entity)

	return mt.sm.Put(key, entity)
}

// Traverse traverses the memTable in order defined by lessFunc
// thread-safe
func (mt *memTable) Traverse(operation func(key []byte, entity *sortedMapEntity)) {
	mt.Lock()
	defer mt.Unlock()

	mt.sm.Traverse(operation)
}
