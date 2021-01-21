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
				log.Printf("[persist] trigger task [%v], immutable queue len [%d]\n", task, lsm.immutableQ.size())
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

	// force GC to release immutable memTable
	runtime.GC()

	return nil
}

// buildSSTFile builds a sstFile from the immutable MemTable
// sstFileName is the UNIQUE identifier of the sstFile
func (lsm *collection) buildSSTFile(sstFileName string, imt *immutableMemTable) (*sstFile, error) {

	log.Printf("[persist] building SST-file[%s]\n", sstFileName)

	// take out ordered entries from immutable memTable
	es := make([]entry, 0, imt.Num())
	imt.Traverse(func(key []byte, entity *sortedMapEntity) {
		es = append(es, entry{
			key:       key,
			value:     entity.value,
			deleteKey: entity.deleteKey,
			meta:      entity.meta,
		})
	})

	file := &sstFile{}

	// meta
	lsm.buildSSTFileMeta(file, es)

	// fd
	file.fd = newMemSSTFileDesc(sstFileName)

	// tiles
	esPages := splitToPages(es, lsm.options.StandardPageSize)
	file.tiles = lsm.PackPagesIntoTile(esPages)

	return file, nil
}

func (lsm *collection) buildSSTFileMeta(file *sstFile, es []entry) {

	var (
		deleteKeyMin  []byte = es[0].deleteKey // init value
		deleteKeyMax  []byte = es[0].deleteKey // init value
		ageOldestTomb uint32 = 0               // init value
		numDelete     int    = 0               // init value
		numEntry      int    = len(es)         // finish value
	)

	dLess := lsm.options.DeleteKeyLess

	for i := 0; i < numEntry; i++ {

		if dLess(es[i].deleteKey, deleteKeyMin) {
			deleteKeyMin = es[i].deleteKey
		}

		if dLess(deleteKeyMax, es[i].deleteKey) {
			deleteKeyMax = es[i].deleteKey
		}

		if es[i].meta.opType == opDel {

			numDelete++

			// parse age of entry from seqNum
			age := uint32((es[i].meta.seqNum >> 32) & 0xFFFFFFFF)
			if ageOldestTomb < age {
				ageOldestTomb = age
			}
		}
	}

	// meta
	file.SortKeyMin = es[0].key
	file.SortKeyMax = es[numEntry-1].key
	file.DeleteKeyMin = deleteKeyMin
	file.DeleteKeyMax = deleteKeyMax
	file.AgeOldestTomb = ageOldestTomb
	file.NumDelete = numDelete
	file.NumEntry = numEntry
}

// splitToPages splits total entries to page-granularity entries according to standardPageSize.
// pure function
// standardPageSize is a recommended value for page size and most pages will be slightly larger than it.
func splitToPages(es []entry, standardPageSize int) (esPages [][]entry) {

	esPages = [][]entry{}

	start := 0
	num := 0
	size := 0

	for i := 0; i < len(es); i++ {

		size += persistFormatLen(&es[i])
		num++

		if size >= standardPageSize || i == len(es)-1 {

			esPages = append(esPages, es[start:start+num])

			// reset
			start += num
			num = 0
			size = 0
		}
	}

	return esPages
}

func (lsm *collection) PackPagesIntoTile(esPages [][]entry) []*deleteTile {

	return nil
}
