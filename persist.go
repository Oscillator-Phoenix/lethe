package lethe

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/google/uuid"
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

	file.fd = newMemSSTFileDesc(sstFileName) // in-memory mock

	file.tiles = []*deleteTile{}

	file.SortKeyMax = []byte{}
	file.SortKeyMin = []byte{}
	file.deleteKeyMax = []byte{}
	file.deleteKeyMin = []byte{}

	file.ageOldestTomb = 0
	file.numDelete = 0

	numEntry := imt.Num()
	ks := make([][]byte, numEntry)
	vs := make([][]byte, numEntry)
	ds := make([][]byte, numEntry)
	metas := make([]keyMeta, numEntry)

	imt.Traverse(func(key []byte, entity *sortedMapEntity) {
		ks = append(ks, key)
		vs = append(vs, entity.value)
		ds = append(ds, entity.deleteKey)
		metas = append(metas, entity.meta)
	})

	log.Printf("[persist] building SST-file[%s]\n", sstFileName)

	return file
}

func (lsm *collection) persistOne() error {
	// pop immutableQ and add SST-file should be packed to an atomic action

	// This locking will block the get from immutableQ
	// TODO OPT: avoid blocking the immutableQ for a long time
	lsm.immutableQ.Lock()

	// the head of queue is the oldest immutable memTable
	imt := lsm.immutableQ.imts[0]

	sstFileName := fmt.Sprintf("%s", uuid.New())
	sstFile := lsm.buildSSTFile(sstFileName, imt) // time cost heavily

	// add the new sstFile to the top peristed level
	lsm.addSSTFileOnLevel(lsm.levels[0], sstFile)

	// when the persistence of head done, pop the head from queue
	lsm.immutableQ.imts = lsm.immutableQ.imts[1:]

	lsm.immutableQ.Unlock()

	runtime.GC() // force GC to release immutable memTable

	return nil
}
