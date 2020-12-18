package lethe

import (
	"bytes"
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
	mu sync.Mutex

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

// setTTL sets TTL for each level in LSM
func (lsm *collection) setTTL() {

	for i := 0; i < len(lsm.levels); i++ {

		ttl := computeTTL(
			lsm.options.DeletePersistThreshold, // D_th
			i+1,                                // levelID = i + 1
			lsm.options.LevelSizeRatio,         // T
			1+len(lsm.levels))                  // L = `Level 0` + `L-1 persisted levels`

		// update TTL of this level
		lsm.levels[i].mu.Lock()
		lsm.levels[i].ttl = ttl
		lsm.levels[i].mu.Unlock()
	}
}

// addNewLevel adds a new level to the bottom of LSM with TTL resetting
func (lsm *collection) addNewLevel() error {
	// LSM lock
	lsm.Lock()
	defer lsm.Unlock()

	lv := newLevel()

	// add a new level
	lsm.levels = append(lsm.levels, lv)

	// re-calculate TTL for each level
	lsm.setTTL()

	log.Printf("add new level %d\n and reset TTL of levels", len(lsm.levels))

	return nil
}

// replaceSSTFileOnLevel replace toRemove files with toInset files on the level
func (lsm *collection) replaceFilesOnLevel(lv *level, toRemove []*sstFile, toInsert []*sstFile) error {
	// Level Lock
	lv.mu.Lock()
	defer lv.mu.Unlock()

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
	lv.mu.Lock()
	defer lv.mu.Unlock()

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

// getFromLevel gets value by key from a level
func (lsm *collection) getFromLevel(lv *level, key []byte) (value []byte) {
	// Level lock
	lv.mu.Lock()
	defer lv.mu.Unlock()

	less := lsm.options.PrimaryKeyLess

	sstFileGet := func(sf *sstFile) []byte {
		// Note that there are no duplicate keys in sstfile, i.e. all keys in a sstFile are unique.

		// get from a page
		pageGet := func(p *page) []byte {
			// page fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
			if less(key, p.primaryKeyMin) || less(p.primaryKeyMax, key) {
				return nil
			}

			// page-granularity bloom filter existence check
			if p.bloomFilterExists(key) == false {
				return nil
			}

			// load data form disk...
			keys, values, _ := p.loadKVs(sf.fd)

			// TODO
			// binary search because entries within every page are sorted on primary key
			for i := 0; i < len(keys); i++ {
				if bytes.Equal(keys[i], key) {
					return values[i]
				}
			}

			return nil
		}

		// get from a delet tile
		deleteTileGet := func(dt *deleteTile) []byte {
			// delet tile fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
			if less(key, dt.primaryKeyMin) || less(dt.primaryKeyMax, key) {
				return nil
			}

			// linear search because pages within a delete tile are sorted on delete key but not primary key
			for i := 0; i < len(dt.pages); i++ {
				if value := pageGet(dt.pages[i]); value != nil {
					return value
				}
			}

			return nil
		}

		// sstFile fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
		if less(key, sf.primaryKeyMin) || less(sf.primaryKeyMax, key) {
			return nil
		}

		// TODO
		// binary search because delete tiles within a sstfile are sorted on primary key
		for i := 0; i < len(sf.tiles); i++ {
			if value := deleteTileGet(sf.tiles[i]); value != nil {
				return value
			}
		}

		return nil
	}

	// levelGet
	// index i : greater(newer file) ==> less(older file)
	// if key is not found in newer file, search in older file.
	for i := len(lv.files) - 1; i >= 0; i-- {
		if value := sstFileGet(lv.files[i]); value != nil {
			return value
		}
	}

	return nil
}

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
