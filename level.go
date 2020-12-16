package lethe

import (
	"bytes"
	"context"
	"math"
	"sync"
	"time"
)

// Key Weaving Storage Layout
// - files contain delete tiles, delete tiles within a file are sorted on primary key
// - delete-tiles contain pages, pages within a delete tile are sorted on delete key
// - entries within every page are sorted on primary key

type level struct {
	levelID int

	primaryKeyLess func(s, t []byte) bool
	deleteKeyLess  func(s, t []byte) bool

	mu    sync.Mutex
	files []*sstFile

	// metadata
	// ttl(time-to-live) is denoted by d_i in paper 4.1.2
	ttl time.Duration

	// compaction
	compactFromPrevNotifier chan struct{}
	compactTrigger          chan compactTask
	levelTTLDaemonCancel    context.CancelFunc
}

func newLevel(levelID int, ttl time.Duration, compactTrigger chan compactTask) *level {
	m := &level{}

	m.levelID = levelID

	m.ttl = ttl

	m.compactTrigger = compactTrigger

	// TODO

	return m
}

func (m *level) close() {
	// TODO
}

func (m *level) get(key []byte) (value []byte) {

	// TODO: data race `compactDaemon` goroutine change files of level

	m.mu.Lock()
	defer m.mu.Unlock()

	pageGet := func(p *page) []byte {

		// fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
		if m.primaryKeyLess(key, p.primaryKeyMin) || m.primaryKeyLess(p.primaryKeyMax, key) {
			return nil
		}

		// page-granularity bloom filter existence check
		if p.bloomFilterExists(key) == false {
			return nil
		}

		// TODO
		// load data form disk...
		keys, values, _ := p.loadKVs()

		// TODO
		// binary search because entries within every page are sorted on primary key
		for i := 0; i < len(keys); i++ {
			if bytes.Equal(keys[i], key) {
				return values[i]
			}
		}

		return nil
	}

	deleteTileGet := func(dt *deleteTile) []byte {
		// fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
		if m.primaryKeyLess(key, dt.primaryKeyMin) || m.primaryKeyLess(dt.primaryKeyMax, key) {
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

	sstFileGet := func(sf *sstFile) []byte {
		// There are no repeated key in a sstFile.

		// fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
		if m.primaryKeyLess(key, sf.primaryKeyMin) || m.primaryKeyLess(sf.primaryKeyMax, key) {
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

	// index i : greater(newer file) ==> less(older file)
	// if key is not found in newer file, search in older file.
	for i := len(m.files); i >= 0; i-- {
		if value := sstFileGet(m.files[i]); value != nil {
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

// updateTTL when a new level is added to LSM
func (m *level) updateTTL(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// update TTL of this level
	m.ttl = ttl

	// update TTL for each file in this level
	for i := len(m.files); i >= 0; i-- {
		m.files[i].updateTTL(m.ttl)
	}
}
