package lethe

import (
	"fmt"
	"sync"
)

type sortedMapEntity struct {
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

type memTable struct {
	sync.Mutex

	_nBytes int
	less    func(s, t []byte) bool
	smm     sortedMap
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
		case constOpPut:
			return "Put"
		case constOpDel:
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

	mt._nBytes = 0
	mt.less = less
	mt.smm = newSkipList(mt.less)

	return mt
}

// ---------------------------------------------------------------------------

// no thread-safe
func (mt *memTable) immutable() *immutableMemTable {
	// imt creates a new sync.Mutex
	imt := &immutableMemTable{}
	imt._nBytes = mt._nBytes
	imt.less = mt.less
	imt.smm = mt.smm
	return imt
}

// no thread safe
func (mt *memTable) reset() {
	mt._nBytes = 0
	mt.smm = newSkipList(mt.less)
}

// nBytes return the number of bytes used
// no thread-safe
func (mt *memTable) nBytes() int {
	return mt._nBytes
}

// ---------------------------------------------------------------------------

// Size returns the number of kv entries in the memTable
// thread-safe
func (mt *memTable) Size() int {
	mt.Lock()
	defer mt.Unlock()

	return mt.smm.Size()
}

// Empty returns whether the memTable is empty
// thread-safe
func (mt *memTable) Empty() bool {
	mt.Lock()
	defer mt.Unlock()

	return mt.smm.Empty()
}

// Get returns the copy of value by key.
// If the key is not found, it returns (nil, false).
// thread-safe
func (mt *memTable) Get(key []byte) (value []byte, found, deleted bool) {
	mt.Lock()
	defer mt.Unlock()

	entity, found := mt.smm.Get(key)

	// key not found
	if !found {
		return nil, false, false
	}

	// found the entity but a tombstone
	if entity.meta.opType == constOpDel { // tombstone
		return nil, false, true
	}

	// found the valid entity
	return copyBytes(entity.value), true, false
}

// Put inserts a kv entry into memTable.
// thread-safe
func (mt *memTable) Put(key, value, deleteKey []byte, meta keyMeta) error {
	mt.Lock()
	defer mt.Unlock()

	mt._nBytes += (len(key) + len(value) + len(deleteKey) + constKeyMetaBytesLen)

	entity := &sortedMapEntity{
		value:     value,
		deleteKey: deleteKey,
		meta:      meta,
	}

	// log.Printf("memTable Put { key[%s] : %v }\n", string(key), entity)

	return mt.smm.Put(key, entity)
}

// Traverse traverses the memTable in order defined by lessFunc
// thread-safe
func (mt *memTable) Traverse(operation func(key []byte, entity *sortedMapEntity)) {
	mt.Lock()
	defer mt.Unlock()

	mt.smm.Traverse(operation)
}
