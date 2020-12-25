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

func (iq *immutableQueue) pop() {
	iq.Lock()
	defer iq.Unlock()

	iq.imts = iq.imts[1:]
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

func (iq *immutableQueue) Get(key []byte) (value []byte, found, deleted bool) {
	iq.Lock()
	defer iq.Unlock()

	// index i : greater(newer) <===> less(older)
	for i := len(iq.imts) - 1; i >= 0; i-- {
		value, found, deleted := iq.imts[i].Get(key)
		if found {
			return value, true, false
		}
		if deleted {
			return nil, false, true
		}
		// if not found and not deleted, keep searching in older immutable memTable.
	}

	return nil, false, false
}

func (lsm *collection) persistDaemon(ctx context.Context) {
	for {
		select {
		case task := <-lsm.persistTrigger:
			{
				log.Printf("[persist daemon] persist trigger task [%v], immutableQueue size [%d]\n", task, lsm.immutableQ.size())
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

	file.fd = newSSTFileDescMock(sstFileName)

	imt.Traverse(func(key []byte, entity *sortedMapEntity) {
		// TODO
	})

	return file
}

func (lsm *collection) persistOne() error {

	// the head of queue is the oldest immutable memTable
	imt := lsm.immutableQ.front()

	file := lsm.buildSSTFile("", imt)

	// TODO / Bugs
	// two operations below should be packed to a atomic behavior
	{
		// add the new sstFile to the top peristed level
		lsm.addSSTFileOnLevel(lsm.levels[0], file)
		// if the persistence of head done, pop the head from queue
		lsm.immutableQ.pop()
	}

	return nil
}
