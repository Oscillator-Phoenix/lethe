package lethe

import "context"

const (
	// TODO: to be a feild of collection.options
	persistTriggerBufLen      = 5
	soCompactionTriggerBufLen = 5
	sdCompactionTriggerBufLen = 5
	ddCompactionTriggerBufLen = 5
)

// A collection implements the Collection interface.
type collection struct {
	// config
	options *CollectionOptions
	stats   *CollectionStats

	// data container
	currentMemTable *memTable
	levels          []level

	// persistence
	// cancel func of persistence, init in Start() and then used in Close()
	persistCancel context.CancelFunc
	// persist trigger
	persistTrigger chan persistTask

	// compaction
	// cancel func of compaction, init in Start() and then used in Close()
	compactCancel context.CancelFunc
	// SO, Saturation-driven trigger and Overlap-driven file selection
	soCompactionTrigger chan compactionTask
	// SD, Saturation-driven trigger and Delete-driven file selection
	sdCompactionTrigger chan compactionTask
	// DD, delete-driven trigger and Delete-driven file selection
	ddCompactionTrigger chan compactionTask
}

func newCollection(options *CollectionOptions) *collection {
	lsm := &collection{}

	lsm.options = options

	lsm.currentMemTable = newMemTable(lsm.options.Less)

	lsm.persistTrigger = make(chan persistTask, persistTriggerBufLen)

	lsm.soCompactionTrigger = make(chan compactionTask, soCompactionTriggerBufLen)
	lsm.sdCompactionTrigger = make(chan compactionTask, sdCompactionTriggerBufLen)
	lsm.ddCompactionTrigger = make(chan compactionTask, ddCompactionTriggerBufLen)

	persistCtx, persistCancel := context.WithCancel(context.Background())
	compactCtx, compactCancel := context.WithCancel(context.Background())

	lsm.persistCancel = persistCancel
	lsm.compactCancel = compactCancel

	go lsm.persistDaemon(persistCtx)
	go lsm.compactDaemon(compactCtx)

	return lsm
}

// Close synchronously stops background goroutines.
func (lsm *collection) Close() error {

	// stop lsm.persistDaemon()
	lsm.persistCancel()

	// stop lsm.compactDaemon()
	lsm.compactCancel()

	return nil
}

// Get retrieves a value by iterating over all the segments within
// the collection, if the key is not found a nil val is returned.
func (lsm *collection) Get(key []byte, readOptions *ReadOptions) ([]byte, error) {

	// lookup on memory table
	if value, isPresent := lsm.currentMemTable.Get(key); isPresent {
		return value, nil
	}

	// lookup on persisted levels
	for i := 0; i < len(lsm.levels); i++ {
		if value := lsm.levels[i].get(key); value != nil {
			return value, nil
		}
	}

	return nil, ErrKeyNotFound
}

// Put creates or updates an key-val entry in the Collection.
func (lsm *collection) Put(key, value []byte, writeOptions *WriteOptions) error {
	// TODO: WAL

	// put KV into memTable
	if err := lsm.currentMemTable.Put(key, value); err != nil {
		return err
	}

	// if the capcity of memTable meet limit
	if lsm.currentMemTable.nBytes() > lsm.options.MemTableBytesLimit {
		// lsm.persistTrigger <- persistTask{
		// 	mt: lsm.currentMemTable,
		// }
		// lsm.currentMemTable = newMemTable(lsm.options.Less)
	}

	return nil
}

// Del deletes a key-val entry from the Collection.
func (lsm *collection) Del(key []byte, writeOptions *WriteOptions) error {

	// TODO
	if err := lsm.currentMemTable.Del(key); err != nil {
		return err
	}

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
