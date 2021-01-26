package lethe

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// KiWi, Key Weaving Storage Layout
// - files contain delete tiles, delete tiles within a file are sorted on sort key
// - delete-tiles contain pages, pages within a delete tile are sorted on delete key
// - entries within every page are sorted on sort key

// ----------------------------------------------------------------------------------------------------------------
// level
// ----------------------------------------------------------------------------------------------------------------

type level struct {
	// lock for level filed
	sync.Mutex `json:"-"`

	// ttl(time-to-live) is denoted by d_i in paper 4.1.2
	ttl int64

	// ---------------------------

	// keep sorted on sort key
	Files []*sstFile

	SizeLimit int
}

// addNewLevel adds a new level to the bottom of LSM and reset TTL for each level
func (lsm *collection) addNewLevel() error {
	// LSM lock
	lsm.Lock()
	defer lsm.Unlock()

	lv := &level{}
	lv.Files = []*sstFile{}

	if len(lsm.levels) == 0 { // level 1
		lv.SizeLimit = lsm.options.LevelSizeRatio * lsm.options.MemTableSizeLimit
	} else { // level 2~(L-1)
		lv.SizeLimit = lsm.options.LevelSizeRatio * lsm.levels[len(lsm.levels)-1].SizeLimit
	}

	// add a new level
	lsm.levels = append(lsm.levels, lv)

	// recalculate TTL for each level
	lsm.setLevelsTTL()

	log.Printf("add a persistent level-%d (limit %s) and recalculate TTLs\n", len(lsm.levels), beautifulNumByte(lsm.levels[len(lsm.levels)-1].SizeLimit))

	return nil
}

// ----------------------------------------------------------------------------------------------------------------
// TTL
// ----------------------------------------------------------------------------------------------------------------

// setLevelsTTL sets TTL for each level in LSM
func (lsm *collection) setLevelsTTL() {

	for i := 0; i < len(lsm.levels); i++ {

		ttl := computeTTL(
			lsm.options.DeletePersistThreshold, // D_th
			i+1,                                // levelID = i + 1
			lsm.options.LevelSizeRatio,         // T
			1+len(lsm.levels))                  // L = `in-memory Level 0` + `L-1 persisted levels`

		// update TTL of this level
		atomic.StoreInt64(&lsm.levels[i].ttl, ttl)
	}
}

// // computeTTL is a pure function computing the TTL of a level
// In paper 4.1.2 Computing d_i
// deletePersistThreshold is denoted by D_th
// levelID is denoted by i
// levelSizeRatio is denoted by T
// numOfLevel is denoted by L
func computeTTL(deletePersistThreshold time.Duration, levelID int, levelSizeRatio int, numOfLevel int) int64 {
	Dth := float64(int(deletePersistThreshold))
	i := float64(levelID)
	T := float64(levelSizeRatio)
	L := float64(numOfLevel)

	d0 := Dth * (T - 1.0) / (math.Pow(T, L-1.0) - 1.0) // assert( math.Pow(T, L-1.0) != 1.0 )
	di := d0 * math.Pow(T, i)

	return int64(di)
}

// ----------------------------------------------------------------------------------------------------------------
// get
// ----------------------------------------------------------------------------------------------------------------

// getFromLevel gets value by key from a level
func (lsm *collection) getFromLevel(lv *level, key []byte) (found bool, value []byte, meta keyMeta) {
	// Level lock
	lv.Lock()
	defer lv.Unlock()

	// levelGet
	// index i : greater(newer file) ==> less(older file)
	// if key is not found in newer file, search in older file.
	for i := len(lv.Files) - 1; i >= 0; i-- {

		if found, value, meta = lsm.getFromSSTFile(lv.Files[i], key); found {
			return true, value, meta
		}

		// If key is not found, keep searching in next sstFile
	}

	return false, nil, meta
}

// -----------------------------------------------------------------------------

// ----------------------------------------------------------------------------------------------------------------
// add a sstFile to level
// ----------------------------------------------------------------------------------------------------------------

// TODO

func (lsm *collection) addFileToLevel(lv *level, file *sstFile) {
	lv.Lock()
	lv.Unlock()

	lv.Files = append(lv.Files, file)

	// sort

	// TODO
	// SO compaction if necessary
}

// -----------------------------------------------------------------------------

// replaceSSTFileOnLevel replace toRemove files with toInset files on the level
func (lsm *collection) replaceFilesOnLevel(lv *level, toRemove []*sstFile, toInsert []*sstFile) error {
	// Level Lock
	lv.Lock()
	defer lv.Unlock()

	if toRemove == nil {
		// do nothing
	} else {
		newFiles := []*sstFile{}

		inToRemove := func(file *sstFile) bool {
			for i := 0; i < len(toRemove); i++ {
				if file == toRemove[i] {
					return true
				}
			}
			return false
		}

		// remove files
		for i := 0; i < len(lv.Files); i++ {
			if !inToRemove(lv.Files[i]) {
				newFiles = append(newFiles, lv.Files[i])
			}
		}
		lv.Files = newFiles
	}

	// add the newest file to the back of level.files
	lv.Files = append(lv.Files, toInsert...)

	return nil
}

// findOverlapFiles returns unsorted the files overlapping with target file.
// If there is no file overlapping with target file, then returns nil.
func (lsm *collection) findOverlapFiles(lv *level, target *sstFile) []*sstFile {
	// Level Lock
	lv.Lock()
	defer lv.Unlock()

	founds := []*sstFile{}

	less := lsm.options.SortKeyLess
	isOverlap := func(f *sstFile) bool {
		return !(less(target.SortKeyMax, f.SortKeyMin) || less(f.SortKeyMax, target.SortKeyMin))
	}

	for i := 0; i < len(lv.Files); i++ {
		if isOverlap(lv.Files[i]) {
			founds = append(founds, lv.Files[i])
		}
	}

	if len(founds) == 0 {
		return nil
	}

	return founds
}

// -----------------------------------------------------------------------------
