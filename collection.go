package lethe

import "context"

// A collection implements the Collection interface.
type collection struct {
	// config
	options *CollectionOptions
	stats   *CollectionStats

	//
	currentMemTable *memTable

	// persistence
	// cancel func of persistence, used in Close()
	persistCancel context.CancelFunc

	// compaction
	// cancel func of compaction, used in Close()
	compactCancel context.CancelFunc
	// SO, Saturation-driven trigger and Overlap-driven file selection
	soCompactionTrigger chan compactionTask
	// SD, Saturation-driven trigger and Delete-driven file selection
	sdCompactionTrigger chan compactionTask
	// DD, delete-driven trigger and Delete-driven file selection
	ddCompactionTrigger chan compactionTask
}

func newCollection(options *CollectionOptions) *collection {
	c := &collection{}

	c.options = options
	c.currentMemTable = newMemTable(c.options.Less)

	return c
}

func (lsm *collection) Start() error {

	persistCtx, persistCancel := context.WithCancel(context.Background())
	compactCtx, compactCancel := context.WithCancel(context.Background())

	lsm.persistCancel = persistCancel
	lsm.compactCancel = compactCancel

	go lsm.persistDaemon(persistCtx)
	go lsm.compactDaemon(compactCtx)

	return nil
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
	if v, isPresent := lsm.currentMemTable.Get(key); isPresent {
		return v, nil
	}

	return nil, ErrKeyNotFound
}

// Put creates or updates an key-val entry in the Collection.
func (lsm *collection) Put(key, value []byte, writeOptions *WriteOptions) error {

	if err := lsm.currentMemTable.Put(key, value); err != nil {
		return err
	}

	if lsm.currentMemTable.nBytes() > lsm.options.MemTableBytesLimit {
		// do flush
		// new memtable
	}

	return nil
}

// Del deletes a key-val entry from the Collection.
func (lsm *collection) Del(key []byte, writeOptions *WriteOptions) error {

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
