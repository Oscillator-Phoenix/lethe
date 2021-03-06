package lethe

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// A collection implements the Collection interface.
type collection struct {

	// protect data field of collection
	sync.Mutex

	// config
	options *CollectionOptions

	// status
	stats *CollectionStats

	// sequentce number of operation on collection
	seqNum uint64

	// in-memory table
	curMemTable      *memTable // `Level 0`

	// persisted levels
	levels []*level // `Level 1` ~ `Level L-1`

	// cancel func of all daemon goroutine, init in Start() and then used in Close()
	daemonCancel context.CancelFunc

	// persistence
	// immutable memTable queue
	immutableQ *immutableQueue
	// persist trigger
	persistTrigger chan persistTask

	// compaction
	// compaction trigger
	compactTrigger chan compactTask
}

func newCollection(options *CollectionOptions) *collection {
	log.Println("new collection")

	lsm := &collection{}

	// set config
	lsm.options = options
	log.Print(lsm.options)

	// create in-memory table, i.e. `Level 0`
	lsm.curMemTable = newMemTable(lsm.options.SortKeyLess)
	log.Printf("add an in-memory level-0 (limit %s)\n", beautifulNumByte(lsm.options.MemTableSizeLimit))

	// create L-1 persist levels, i.e. `Level 1` ~ `Level L-1`
	lsm.levels = []*level{}
	for i := 0; i < lsm.options.NumInitialLevel-1; i++ {
		lsm.addNewLevel()
	}

	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	lsm.daemonCancel = daemonCancel

	// persist daemon
	lsm.immutableQ = newImmutableQueue()
	lsm.persistTrigger = make(chan persistTask, lsm.options.persistTriggerBufLen)
	go lsm.persistDaemon(daemonCtx)

	// compact daemon
	lsm.compactTrigger = make(chan compactTask, lsm.options.compactTriggerBufLen)
	go lsm.compactDaemon(daemonCtx)

	// time stamp update daemon
	lsm.resetSeqNumNForNow()
	go lsm.timeStampUpdateDaemon(daemonCtx)

	return lsm
}

// Close synchronously stops background goroutines.
func (lsm *collection) Close() error {
	log.Println("collection is closing ...")

	// stop all daemon goroutine
	lsm.daemonCancel()

	log.Println("collection is closed")

	time.Sleep(time.Second * 1)

	return nil
}

// time stamp update daemon
func (lsm *collection) timeStampUpdateDaemon(ctx context.Context) {

	ticker := time.NewTicker(1 * time.Minute) // 1 minutes ticker
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			{
				lsm.resetSeqNumNForNow()
			}
		case <-ctx.Done():
			{
				log.Println("stop [time stamp update daemon]")
				return
			}
		}
	}
}

// reset seqNum using current real time
func (lsm *collection) resetSeqNumNForNow() {
	// high 32: now Unix time stamp in seconds
	// low  32: 0
	seqNum := ((uint64(time.Now().Unix()) & 0xFFFFFFFF) << 32) | (0 & 0xFFFFFFFF)
	atomic.StoreUint64(&lsm.seqNum, seqNum)
}

// getSeqNum atomically increates `lsm.seqNumInc` and returns the new value.
// Unix-like operating systems often record time as a 32-bit count of seconds.
// Here, lsm.timeStamp is valid for one hundred years until 2100 A.D., then it will overflow.
func (lsm *collection) getSeqNum() uint64 {
	return atomic.AddUint64(&lsm.seqNum, 1)
}

// Get retrieves a value by iterating over all the segments within
// the collection, if the key is not found a nil val is returned.
func (lsm *collection) Get(key []byte, readOptions *ReadOptions) ([]byte, error) {

	if len(key) > maxSortKeyBytesLen {
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

	if len(key) > maxSortKeyBytesLen {
		return ErrSortKeyTooLarge
	}
	if len(deleteKey) > maxDeleteKeyBytesLen {
		return ErrDeleteKeyTooLarge
	}
	if len(value) > maxValueBytesLen {
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

	if len(key) > maxSortKeyBytesLen {
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
