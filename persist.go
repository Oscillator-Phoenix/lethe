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
				log.Println("stop [persist daemon]")
				return
			}
		}
	}
}

// buildSSTFile builds a sstFile from the immutableMemTable
// sstFileName is the UNIQUE identifier of the sstFile
func (lsm *collection) buildSSTFile(sstFileName string, imt *immutableMemTable) (*sstFile, error) {

	log.Printf("[persist] building SST-file[%s]\n", sstFileName)

	// sLess := lsm.options.SortKeyLess
	dLess := lsm.options.DeleteKeyLess

	numEntry := imt.Num()
	ks := make([][]byte, 0, numEntry)
	vs := make([][]byte, 0, numEntry)
	ds := make([][]byte, 0, numEntry)
	metas := make([]keyMeta, 0, numEntry)

	// take out data from immutable memTable
	imt.Traverse(func(key []byte, entity *sortedMapEntity) {
		ks = append(ks, key)
		vs = append(vs, entity.value)
		ds = append(ds, entity.deleteKey)
		metas = append(metas, entity.meta)
	})

	var (
		deleteKeyMin  []byte = ds[0] // init value
		deleteKeyMax  []byte = ds[0] // init value
		numDelete     int    = 0     // init value
		ageOldestTomb uint32 = 0     // intt value
	)

	for i := 0; i < numEntry; i++ {
		if dLess(ds[i], deleteKeyMin) {
			deleteKeyMin = ds[i]
		}
		if dLess(deleteKeyMax, ds[i]) {
			deleteKeyMax = ds[i]
		}
		if metas[i].opType == opDel {
			numDelete++

			// parse age of entry from seqNum
			age := uint32((metas[i].seqNum >> 32) & 0xFFFFFFFF)
			if ageOldestTomb < age {
				ageOldestTomb = age
			}
		}
	}

	file := &sstFile{}

	// meta
	file.SortKeyMin = ks[0]
	file.SortKeyMax = ks[numEntry-1]
	file.DeleteKeyMin = deleteKeyMin
	file.DeleteKeyMax = deleteKeyMax
	file.AgeOldestTomb = ageOldestTomb
	file.NumDelete = numDelete
	file.NumEntry = numEntry

	file.fd = newMemSSTFileDesc(sstFileName) // init, in-memory mock
	file.tiles = []*deleteTile{}             // init

	// TODO
	// lsm.bulidPages(ks, vs, ds, metas)
	// lsm.buildDeleteTile()

	return file, nil
}

func (lsm *collection) persistOne() error {
	// pop immutableQ and add SST-file should be packed to an atomic action

	// This locking will block the get from immutableQ
	// TODO OPT: avoid blocking the immutableQ for a long time
	lsm.immutableQ.Lock()

	// the head of queue is the oldest immutable memTable
	imt := lsm.immutableQ.imts[0]

	sstFileName := fmt.Sprintf("%s", uuid.New())
	sstFile, _ := lsm.buildSSTFile(sstFileName, imt) // time cost heavily

	// add the new sstFile to the top peristed level
	lsm.addSSTFileOnLevel(lsm.levels[0], sstFile)

	// when the persistence of head done, pop the head from queue
	lsm.immutableQ.imts = lsm.immutableQ.imts[1:]

	lsm.immutableQ.Unlock()

	runtime.GC() // force GC to release immutable memTable

	return nil
}
