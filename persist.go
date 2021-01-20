package lethe

import (
	"context"
	"log"
	"sync"
)

type persistTask struct{}

type immutableQueue struct {
	sync.Mutex
	imts []*immutableMemTable
}

func newImmutableQueue() *immutableQueue {
	iq := &immutableQueue{}
	iq.imts = []*immutableMemTable{}
	return iq
}

func (iq *immutableQueue) push(imt *immutableMemTable) {
	iq.Lock()
	defer iq.Unlock()

	iq.imts = append(iq.imts, imt)
}

func (iq *immutableQueue) front() *immutableMemTable {
	iq.Lock()
	defer iq.Unlock()

	return iq.imts[0]
}

// size returns the number of immutable memTables in immutableQueue
func (iq *immutableQueue) size() int {
	iq.Lock()
	defer iq.Unlock()

	return len(iq.imts)
}

func (iq *immutableQueue) Get(key []byte) (found bool, value []byte, meta keyMeta) {
	iq.Lock()
	defer iq.Unlock()

	// index i : greater(newer) <===> less(older)
	for i := len(iq.imts) - 1; i >= 0; i-- {

		if found, value, meta = iq.imts[i].Get(key); found {
			return true, value, meta
		}

		// if key is not found, keep searching in older immutable memTables.
	}

	return false, nil, meta
}

func (lsm *collection) persistDaemon(ctx context.Context) {
	for {
		select {
		case task := <-lsm.persistTrigger:
			{
				log.Printf("[persist] trigger task [%v], immutableQueue size [%d]\n", task, lsm.immutableQ.size())
				lsm.persistOne()
			}
		case <-ctx.Done():
			{
				log.Println("stop persist daemon")
				return
			}
		}
	}
}

// buildSSTFile builds a sstFile from the immutableMemTable
// sstFileName is the UNIQUE identifier of the sstFile
func (lsm *collection) buildSSTFile(sstFileName string, imt *immutableMemTable) *sstFile {
	file := &sstFile{}

	// file.fd = newMemSSTFileDesc(sstFileName) // in-memory
	// file.tiles = []*deleteTile{}

	// file.SortKeyMax = []byte{}
	// file.SortKeyMin = []byte{}
	// file.deleteKeyMax = []byte{}
	// file.deleteKeyMin = []byte{}

	// file.aMax = 0
	// file.b = 0

	// imt.Traverse(func(key []byte, entity *sortedMapEntity) {
	// 	// TODO
	// })

	log.Printf("[persist] building SST-file [%s]\n", sstFileName)

	return file
}

func (lsm *collection) persistOne() error {

	// the head of queue is the oldest immutable memTable
	imt := lsm.immutableQ.front()

	// time cost heavily
	file := lsm.buildSSTFile("", imt)

	// two operations below should be packed to a atomic action
	{
		lsm.immutableQ.Lock()

		// add the new sstFile to the top peristed level
		lsm.addSSTFileOnLevel(lsm.levels[0], file)

		lsm.immutableQ.imts = lsm.immutableQ.imts[1:] // pop the head

		// if the persistence of head done, pop the head from queue
		lsm.immutableQ.Unlock()
	}

	return nil
}
