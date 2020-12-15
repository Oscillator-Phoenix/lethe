package lethe

import (
	"context"
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
	levelID int
	mu      sync.Mutex
	files   []sstFile

	// metadata
	// ttl(time-to-live) is denoted by d_i in paper 4.1.2
	ttl time.Duration

	// compaction

	compactFromPrevNotifier chan struct{}
	soCompactionTrigger     chan compactionTask
	sdCompactionTrigger     chan compactionTask
	ddCompactionTrigger     chan compactionTask
	levelTTLDaemonCancel    context.CancelFunc
}

func newLevel(levelID int, ttl time.Duration, so, sd, dd chan compactionTask) *level {
	m := &level{}

	m.levelID = levelID

	m.ttl = ttl

	m.compactFromPrevNotifier = make(chan struct{}, 1) // notifier should be async, required buffer

	m.soCompactionTrigger = so
	m.sdCompactionTrigger = sd
	m.ddCompactionTrigger = dd

	ctx, cancel := context.WithCancel(context.Background())
	m.levelTTLDaemonCancel = cancel
	go m.levelTTLDaemon(ctx)

	return m
}

func (m *level) close() {
	m.levelTTLDaemonCancel()
}

func (m *level) compactFromPrevNotify() {
	m.compactFromPrevNotifier <- struct{}{}
}

// TTL
func (m *level) levelTTLDaemon(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			{
				log.Printf("stop levelTTLDaemon from Level %d\n", m.levelID)
				return
			}
		}
	}
}

func (m *level) get(key []byte) (value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// index : greater(newer file) ==> less(older file)
	// if key is not found in newer file, search in older file, else retrun.
	for i := len(m.files); i >= 0; i-- {
		if value := m.files.get(key); value != nil {
			return value
		}
	}

	return nil
}

// In paper 4.1.2 Computing d_i
// deletePersistThreshold is denoted by D_th in paper 4.1.2
// levelID is denoted by i in paper 4.1.2
// levelSizeRatio is denoted by  T in paper 4.1.2
// numOfLevel is denoted by L in paper 4.1.2
// As the paper says, lethe re-calculates ttl after every buffer flush.
func computeTTL(deletePersistThreshold time.Duration, levelID int, levelSizeRatio int, numOfLevel int) time.Duration {
	Dth := float64(int(deletePersistThreshold))
	i := float64(levelID)
	T := float64(levelSizeRatio)
	L := float64(numOfLevel)

	d0 := Dth * (T - 1.0) / (math.Pow(T, L-1.0) - 1.0) // assert( math.Pow(T, L-1.0) != 1.0 )
	di := d0 * math.Pow(T, i)

	return time.Duration(int(di))
}

func (m *level) updateTTL(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ttl = ttl
}
