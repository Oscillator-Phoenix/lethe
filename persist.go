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

type persistEntries struct {
	num              int
	keys             [][]byte
	values           [][]byte
	deleteKeys       [][]byte
	metas            []keyMeta
	persistFormatBuf []byte
}

func newPersistEntries(num int) *persistEntries {
	pes := &persistEntries{}
	pes.num = num
	pes.keys = make([][]byte, 0, num)
	pes.values = make([][]byte, 0, num)
	pes.deleteKeys = make([][]byte, 0, num)
	pes.metas = make([]keyMeta, 0, num)
	return pes
}

// buildSSTFile builds a sstFile from the immutable MemTable
// sstFileName is the UNIQUE identifier of the sstFile
func (lsm *collection) buildSSTFile(sstFileName string, imt *immutableMemTable) (*sstFile, error) {

	log.Printf("[persist] building SST-file[%s]\n", sstFileName)

	pes := newPersistEntries(imt.Num())

	// take out ordered entries from immutable memTable
	imt.Traverse(func(key []byte, entity *sortedMapEntity) {
		pes.keys = append(pes.keys, key)
		pes.values = append(pes.values, entity.value)
		pes.deleteKeys = append(pes.deleteKeys, entity.deleteKey)
		pes.metas = append(pes.metas, entity.meta)

		if buf, err := encodeEntry(key, entity.value, entity.deleteKey, entity.meta); err == nil {
			pes.persistFormatBuf = buf
		}
	})

	file := &sstFile{}

	// meta
	lsm.buildSSTFileMeta(file, pes)

	// fd
	file.fd = newMemSSTFileDesc(sstFileName)

	// tiles
	pesPages := lsm.splitToPages(pes)
	file.tiles = lsm.PackPagesIntoTile(pesPages)

	return file, nil
}

func (lsm *collection) buildSSTFileMeta(file *sstFile, pes *persistEntries) {

	var (
		deleteKeyMin  []byte = pes.deleteKeys[0] // init value
		deleteKeyMax  []byte = pes.deleteKeys[0] // init value
		ageOldestTomb uint32 = 0                 // init value
		numDelete     int    = 0                 // init value
		numEntry      int    = pes.num           // finish value
	)

	dLess := lsm.options.DeleteKeyLess

	for i := 0; i < numEntry; i++ {

		if dLess(pes.deleteKeys[i], deleteKeyMin) {
			deleteKeyMin = pes.deleteKeys[i]
		}

		if dLess(deleteKeyMax, pes.deleteKeys[i]) {
			deleteKeyMax = pes.deleteKeys[i]
		}

		if pes.metas[i].opType == opDel {

			numDelete++

			// parse age of entry from seqNum
			age := uint32((pes.metas[i].seqNum >> 32) & 0xFFFFFFFF)
			if ageOldestTomb < age {
				ageOldestTomb = age
			}
		}
	}

	// meta
	file.SortKeyMin = pes.keys[0]
	file.SortKeyMax = pes.keys[numEntry-1]
	file.DeleteKeyMin = deleteKeyMin
	file.DeleteKeyMax = deleteKeyMax
	file.AgeOldestTomb = ageOldestTomb
	file.NumDelete = numDelete
	file.NumEntry = numEntry
}

func (lsm *collection) splitToPages(pes *persistEntries) (pesPages []persistEntries) {
	return nil
}

func (lsm *collection) PackPagesIntoTile(pes []persistEntries) []*deleteTile {
	return nil
}
