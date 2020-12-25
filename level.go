package lethe

import (
	"log"
	"math"
	"sync"
	"time"
)

// Key Weaving Storage Layout
// - files contain delete tiles, delete tiles within a file are sorted on primary key
// - delete-tiles contain pages, pages within a delete tile are sorted on delete key
// - entries within every page are sorted on primary key

type level struct {
	// lock for level filed
	sync.Mutex

	files []*sstFile

	// ttl(time-to-live) is denoted by d_i in paper 4.1.2
	ttl time.Duration
}

// newLevel returns a new level holding no sstFile
func newLevel() *level {
	lv := &level{}

	lv.files = []*sstFile{}

	return lv
}

// -----------------------------------------------------------------------------

// computeTTL is a pure function computing the TTL of a level
// In paper 4.1.2 Computing d_i
// deletePersistThreshold is denoted by D_th
// levelID is denoted by i
// levelSizeRatio is denoted by T
// numOfLevel is denoted by L
func computeTTL(deletePersistThreshold time.Duration, levelID int, levelSizeRatio int, numOfLevel int) time.Duration {
	Dth := float64(int(deletePersistThreshold))
	i := float64(levelID)
	T := float64(levelSizeRatio)
	L := float64(numOfLevel)

	d0 := Dth * (T - 1.0) / (math.Pow(T, L-1.0) - 1.0) // assert( math.Pow(T, L-1.0) != 1.0 )
	di := d0 * math.Pow(T, i)

	return time.Duration(int(di))
}

// -----------------------------------------------------------------------------

// setTTL sets TTL for each level in LSM
func (lsm *collection) setTTL() {

	for i := 0; i < len(lsm.levels); i++ {

		ttl := computeTTL(
			lsm.options.DeletePersistThreshold, // D_th
			i+1,                                // levelID = i + 1
			lsm.options.LevelSizeRatio,         // T
			1+len(lsm.levels))                  // L = `Level 0` + `L-1 persisted levels`

		// update TTL of this level
		lsm.levels[i].Lock()
		lsm.levels[i].ttl = ttl
		lsm.levels[i].Unlock()
	}
}

// addNewLevel adds a new level to the bottom of LSM and reset TTL for each level
func (lsm *collection) addNewLevel() error {
	// LSM lock
	lsm.Lock()
	defer lsm.Unlock()

	lv := newLevel()

	// add a new level
	lsm.levels = append(lsm.levels, lv)

	// re-calculate TTL for each level
	lsm.setTTL()

	log.Printf("add new level %d (persist) and reset TTL of levels", len(lsm.levels))

	return nil
}

func (lsm *collection) addSSTFileOnLevel(lv *level, file *sstFile) {
	lv.Lock()
	lv.Unlock()

	lv.files = append(lv.files, file)
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
		for i := 0; i < len(lv.files); i++ {
			if !inToRemove(lv.files[i]) {
				newFiles = append(newFiles, lv.files[i])
			}
		}
		lv.files = newFiles
	}

	// add the newest file to the back of level.files
	lv.files = append(lv.files, toInsert...)

	return nil
}

// findOverlapFiles returns unsorted the files overlapping with target file.
// If there is no file overlapping with target file, then returns nil.
func (lsm *collection) findOverlapFiles(lv *level, target *sstFile) []*sstFile {
	// Level Lock
	lv.Lock()
	defer lv.Unlock()

	founds := []*sstFile{}

	less := lsm.options.PrimaryKeyLess
	isOverlap := func(f *sstFile) bool {
		return !(less(target.primaryKeyMax, f.primaryKeyMin) || less(f.primaryKeyMax, target.primaryKeyMin))
	}

	for i := 0; i < len(lv.files); i++ {
		if isOverlap(lv.files[i]) {
			founds = append(founds, lv.files[i])
		}
	}

	if len(founds) == 0 {
		return nil
	}

	return founds
}

// -----------------------------------------------------------------------------

// getFromLevel gets value by key from a level
func (lsm *collection) getFromLevel(lv *level, key []byte) (value []byte, found, deleted bool) {
	// Level lock
	lv.Lock()
	defer lv.Unlock()

	// levelGet
	// index i : greater(newer file) ==> less(older file)
	// if key is not found in newer file, search in older file.
	for i := len(lv.files) - 1; i >= 0; i-- {
		value, found, deleted := lsm.getFromSSTFile(lv.files[i], key)
		if found {
			return value, true, false
		}
		if deleted {
			return nil, false, true
		}
		// If key is not found and not deleted, keep searching in next sstFile
	}

	return nil, false, false
}
