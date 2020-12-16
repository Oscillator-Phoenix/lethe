package lethe

import (
	"context"
	"log"
	"sync"
)

// A collection implements the Collection interface.
type collection struct {
	// config
	options *CollectionOptions
	stats   *CollectionStats

	// in-memory table
	curMemTable      *memTable
	curMemTableMutex sync.Mutex

	// persisted levels
	levels []*level

	// persistence
	// cancel func of persistence, init in Start() and then used in Close()
	persistCancel context.CancelFunc
	// persist trigger
	persistTrigger chan persistTask

	// compaction
	// cancel func of compaction, init in Start() and then used in Close()
	compactCancel  context.CancelFunc
	compactTrigger chan compactTask
}

func newCollection(options *CollectionOptions) *collection {
	log.Println("new collection")

	lsm := &collection{}

	lsm.options = options

	lsm.curMemTable = newMemTable(lsm.options.PrimaryKeyLess)

	lsm.levels = []*level{}
	for i := 0; i < lsm.options.InitialLevelNum-1; i++ {
		lsm.addNewLevel()
	}

	// persist
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
	if value, isPresent := lsm.curMemTable.Get(key); isPresent {
		return value, nil
	}

	// lookup on persisted levels with disk IO
	// index i : less(newer) <===> greater(older)
	for i := 0; i < len(lsm.levels); i++ {
		if value := lsm.getFromLevel(lsm.levels[i], key); value != nil {
			return value, nil
		}
	}

	return nil, ErrKeyNotFound
}

// Put creates or updates an key-val entry in the Collection.
func (lsm *collection) Put(key, value, dKey []byte, writeOptions *WriteOptions) error {

	// add lock to prevent from Put while changing curMemTable
	lsm.curMemTableMutex.Lock()
	mt := lsm.curMemTable
	lsm.curMemTableMutex.Unlock()

	// put KV into memTable
	if err := mt.Put(key, value); err != nil {
		return err
	}

	// TODO

	// // if the capcity of memTable meet limit
	// if lsm.curMemTable.nBytes() > lsm.options.MemTableBytesLimit {

	// 	// create a new sstFile

	// 	// commit a perstist task
	// 	// lsm.persistTrigger <- persistTask{
	// 	// 	mt: lsm.curMemTable,
	// 	// }

	// 	file := lsm.levels[0].addSSTFile(lsm.curMemTable)

	// 	lsm.curMemTableMutex.Lock()
	// 	lsm.curMemTable = newMemTable(lsm.options.PrimaryKeyLess)
	// 	lsm.curMemTableMutex.Unlock()
	// }

	return nil
}

func (lsm *collection) compactIfNecessary() {

	// for i := 0; i < len(lsm.levels)-1; i++ {
	// 	if lsm.levels[i].nBytes() > limit {
	// 		lsm.soCompactionTrigger <- compactTask{
	// 			curLevel: lsm.levels[i],
	// 			nextLevel:lsm.levels[i+1]
	// 		}
	// 	}
	// }
}

// Del deletes a key-val entry from the Collection.
func (lsm *collection) Del(key []byte, writeOptions *WriteOptions) error {

	// TODO
	if err := lsm.curMemTable.Del(key); err != nil {
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
