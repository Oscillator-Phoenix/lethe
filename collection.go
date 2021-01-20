package lethe

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
)

const (
	constMaxSortKeyBytesLen   int = (1 << 16) - 1
	constMaxDeleteKeyBytesLen int = (1 << 16) - 1
	constMaxValueBytesLen     int = (1 << 32) - 1
)

// A collection implements the Collection interface.
type collection struct {

	// protect data field of collection
	sync.Mutex

	// config
	options *CollectionOptions

	// status
	stats *CollectionStats

	// increater sequentce number of operation on collection
	seqNumInc uint64

	// in-memory table
	curMemTable      *memTable // `Level 0`
	curMemTableMutex sync.Mutex

	// persisted levels
	levels []*level // `Level 1` ~ `Level L-1`

	// persistence
	// immutable memTable queue
	immutableQ *immutableQueue
	// persist trigger
	persistTrigger chan persistTask
	// cancel func of persistence, init in Start() and then used in Close()
	persistCancel context.CancelFunc

	// compaction
	// compaction trigger
	compactTrigger chan compactTask
	// cancel func of compaction, init in Start() and then used in Close()
	compactCancel context.CancelFunc
}

func newCollection(options *CollectionOptions) *collection {
	log.Println("new collection")

	lsm := &collection{}

	// set config
	lsm.options = options
	log.Print(lsm.options)

	// create in-memory table, i.e. `Level 0`
	lsm.curMemTable = newMemTable(lsm.options.SortKeyLess)
	log.Println("add new level 0 which is an in-memory table")

	// create L-1 persist levels, i.e. `Level 1` ~ `Level L-1`
	lsm.levels = []*level{}
	for i := 0; i < lsm.options.InitialLevelNum-1; i++ {
		lsm.addNewLevel()
	}

	// persist
	lsm.immutableQ = newImmutableQueue()
	lsm.persistTrigger = make(chan persistTask, lsm.options.persistTriggerBufLen)
	persistCtx, persistCancel := context.WithCancel(context.Background())
	lsm.persistCancel = persistCancel
	go lsm.persistDaemon(persistCtx)

	// compact
	lsm.compactTrigger = make(chan compactTask, lsm.options.compactTriggerBufLen)
	compactCtx, compactCancel := context.WithCancel(context.Background())
	lsm.compactCancel = compactCancel
	go lsm.compactDaemon(compactCtx)

	return lsm
}

// Close synchronously stops background goroutines.
func (lsm *collection) Close() error {
	log.Println("collection is closing ...")

	// stop lsm.persistDaemon()
	lsm.persistCancel()

	// stop lsm.compactDaemon()
	lsm.compactCancel()

	log.Println("collection is closed")

	return nil
}

// getSeqNum atomically increates `lsm.seqNumInc` and returns the new value
func (lsm *collection) getSeqNum() uint64 {
	return atomic.AddUint64(&lsm.seqNumInc, 1)
}

// Get retrieves a value by iterating over all the segments within
// the collection, if the key is not found a nil val is returned.
func (lsm *collection) Get(key []byte, readOptions *ReadOptions) ([]byte, error) {

	if len(key) > constMaxSortKeyBytesLen {
		return nil, ErrSortKeyTooLarge
	}

	var (
		found bool
		value []byte
		meta  keyMeta
	)

	// look up on current memTable
	found, value, meta = lsm.curMemTable.Get(key)

	// look up on immutable memTable queue
	if !found {
		found, value, meta = lsm.immutableQ.Get(key)
	}

	// loop up on persisted levels
	if !found {

		// index i : less(newer) <===> greater(older)
		for i := 0; i < len(lsm.levels); i++ {

			found, value, meta = lsm.getFromLevel(lsm.levels[i], key)

			if found {
				break
			}
			// else keep searching in next older level
		}
	}

	// key is not found through LSM
	if !found {
		return nil, ErrKeyNotFound
	}

	// found the entity but a tombstone
	if meta.opType == opDel {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

// Put creates or updates an key-val entry in the Collection.
func (lsm *collection) Put(key, value, deleteKey []byte, writeOptions *WriteOptions) error {

	if len(key) > constMaxSortKeyBytesLen {
		return ErrSortKeyTooLarge
	}
	if len(deleteKey) > constMaxDeleteKeyBytesLen {
		return ErrDeleteKeyTooLarge
	}
	if len(value) > constMaxValueBytesLen {
		return ErrValueTooLarge
	}

	meta := keyMeta{
		seqNum: lsm.getSeqNum(), // atomic
		opType: opPut,           // Put
	}

	// put KV into memTable
	if err := lsm.curMemTable.Put(key, value, deleteKey, meta); err != nil { // lsm.curMemTable lock
		return err
	}

	lsm.resetCurMemTableIfNecessary()

	return nil
}

func (lsm *collection) resetCurMemTableIfNecessary() {

	// if the size of memTable meets the limit, then trigger a persist

	if reset, imt := lsm.curMemTable.resetIfNecessary(lsm.options.MemTableSizeLimit); reset {
		// add immutable memTable to queue
		lsm.immutableQ.push(imt)

		// one immutable triggers one persist task
		lsm.persistTrigger <- persistTask{}
	}
}

// Del deletes a key-val entry from the Collection.
func (lsm *collection) Del(key []byte, writeOptions *WriteOptions) error {

	if len(key) > constMaxSortKeyBytesLen {
		return ErrSortKeyTooLarge
	}

	meta := keyMeta{
		seqNum: lsm.getSeqNum(), // atomic
		opType: opDel,           // Del, tombstone
	}

	// put tombstone into memTable
	if err := lsm.curMemTable.Put(key, nil, nil, meta); err != nil { // lsm.curMemTable lock
		return err
	}

	lsm.resetCurMemTableIfNecessary()

	return nil
}

// RangeDel deletes key-val entry ranged [lowKey, highKey]
func (lsm *collection) RangeDel(lowKey, highKey []byte, writeOptions *WriteOptions) error {
	return nil
}

// Options returns the current options.
func (lsm *collection) Options() CollectionOptions {
	// TODO
	return *lsm.options
}

// Stats returns stats for this collection.
func (lsm *collection) Stats() (*CollectionStats, error) {
	// TODO
	cs := &CollectionStats{}
	return cs, nil
}
