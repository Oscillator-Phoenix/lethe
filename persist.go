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
		case <-lsm.persistTrigger:
			{
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

	log.Printf("[persist] trigger, immutable queue len [%d]\n", lsm.immutableQ.size())

	// pop immutableQ and add SST-file should be packed to an atomic action

	// This locking will block the get from immutableQ
	// TODO OPT: avoid blocking the immutableQ for a long time
	lsm.immutableQ.Lock()

	// the head of queue is the oldest immutable memTable
	imt := lsm.immutableQ.imts[0]

	// take out entries sorted on sortKey from immutable memTable
	es := make([]entry, 0, imt.Num())
	imt.Traverse(func(key []byte, entity *sortedMapEntity) {
		es = append(es, entry{
			key:       key,
			value:     entity.value,
			deleteKey: entity.deleteKey,
			meta:      entity.meta,
		})
	})

	sstFileName := fmt.Sprintf("%s", uuid.New())
	sstFile, _ := lsm.buildSSTFile(sstFileName, es) // time cost heavily

	// add the new sstFile to the top peristed level
	lsm.addFileToLevel(lsm.levels[0], sstFile)

	// when the persistence of head done, pop the head from queue
	lsm.immutableQ.imts = lsm.immutableQ.imts[1:]

	lsm.immutableQ.Unlock()

	// force GC to release immutable memTable
	runtime.GC()

	log.Printf("[persist] build SST-file [%s]\n", sstFileName)

	return nil
}

type persistPage struct {
	p  page
	es []entry
}

type persistTile struct {
	tile   deleteTile
	ppages []persistPage
}

// buildSSTFile builds a sstFile from entries
// sstFileName is the UNIQUE identifier of the sstFile
// require: the input []entry is sorted on sortKey
func (lsm *collection) buildSSTFile(sstFileName string, es []entry) (*sstFile, error) {

	file := &sstFile{}

	// now es is sorted on sortKey
	// note that `buildSSTFileMeta` will NOT change the order of es
	lsm.buildSSTFileMeta(file, es)

	// now es is sorted on sortKey
	// note that `splitToTiles` will change the order of es
	pts := lsm.splitToTiles(es)

	// open fd via unique name
	file.fd = openMemSSTFileDesc(sstFileName) // mock

	// pack
	lsm.packTilesIntoFile(file, pts)

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

// -------------------------------------------------------------------------------

func divUp(dividend, divisor int) int {
	return (dividend + divisor - 1) / divisor
}

func entriesTotalSize(es []entry) int {
	sum := 0
	for i := 0; i < len(es); i++ {
		sum += persistFormatLen(&es[i])
	}
	return sum
}

func entriesAvgSize(es []entry) int {
	return divUp(entriesTotalSize(es), len(es))
}

// require: input `es` is sorted on deleteKey
func (lsm *collection) splitToPages(es []entry) []persistPage {

	pps := []persistPage{}

	start := 0
	num := 0
	size := 0

	// divide into pages on the delete key
	for i := 0; i < len(es); i++ {

		size += persistFormatLen(&es[i])
		num++

		if size >= lsm.options.StandardPageSize || i == len(es)-1 {

			// construct a page
			{
				var pp persistPage

				esPage := es[start : start+num]

				// now esPage is sorted on delete key
				pp.p.DeleteKeyMin = esPage[0].deleteKey
				pp.p.DeleteKeyMax = esPage[len(esPage)-1].deleteKey

				sortEntriesOnSortKey(esPage, lsm.options.SortKeyLess)

				// now esPage is sorted on sort key
				pp.p.SortKeyMin = esPage[0].key
				pp.p.SortKeyMax = esPage[len(esPage)-1].key

				// esPage should be sorted on sort key
				// note that the order of entries in `esPage` can not be changed anymore
				pp.es = esPage

				pps = append(pps, pp)
			}

			// reset
			start += num
			num = 0
			size = 0
		}
	}

	return pps
}

// require: input `es` is sorted on sort key
func (lsm *collection) splitToTiles(es []entry) []persistTile {

	standardTileSize := lsm.options.NumPagePerDeleteTile * lsm.options.StandardPageSize
	approximateNumEntryInTile := divUp(standardTileSize, entriesAvgSize(es))

	pts := []persistTile{}

	// divide into delete-tiles on the sort key
	for start := 0; start < len(es); start += approximateNumEntryInTile {

		end := start + approximateNumEntryInTile
		if end > len(es) {
			end = len(es)
		}

		esTile := es[start:end]

		{
			var pt persistTile

			// now esTile is sorted on sort key
			pt.tile.SortKeyMin = esTile[0].key
			pt.tile.SortKeyMax = esTile[len(esTile)-1].key

			sortEntriesOnDeleteKey(esTile, lsm.options.DeleteKeyLess)

			// now esTile is sorted on delete key
			pt.tile.DeleteKeyMin = esTile[0].deleteKey
			pt.tile.DeleteKeyMax = esTile[len(esTile)-1].deleteKey

			pt.ppages = lsm.splitToPages(esTile)
			pts = append(pts, pt)
		}
	}

	return pts
}

func (lsm *collection) packTilesIntoFile(file *sstFile, pts []persistTile) error {

	var (
		off int64 = 0
	)

	file.Tiles = make([]deleteTile, len(pts))

	for i := 0; i < len(pts); i++ {

		pt := &pts[i]

		pt.tile.Pages = make([]page, len(pt.ppages))

		for j := 0; j < len(pt.ppages); j++ {

			// encode
			buf, err := encodeEntries(pt.ppages[j].es)
			if err != nil {
				return err
			}

			// write
			n, err := file.fd.Write(buf)
			if n != len(buf) {
				return ErrPlaceholder
			}
			if err != nil {
				return err
			}

			// record size and offset
			pt.ppages[j].p.Size = int64(len(buf))
			pt.ppages[j].p.Offset = off

			// the page is is assembled completely
			pt.tile.Pages[j] = pt.ppages[j].p

			// update offset
			off += int64(n)
		}

		// the delelte-tile is assembled completely
		file.Tiles[i] = pt.tile
	}

	return nil
}
