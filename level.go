package lethe

import (
	"context"
	"log"
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
	d float64

	// compaction
	ttlTicker               *time.Ticker
	compactFromPrevNotifier chan struct{}
	soCompactionTrigger     chan compactionTask
	sdCompactionTrigger     chan compactionTask
	ddCompactionTrigger     chan compactionTask
	ttlDaemonCancel         context.CancelFunc
}

func newLevel(levelID int, so, sd, dd chan compactionTask) *level {
	m := &level{}

	m.levelID = levelID

	m.d = m.computeD()
	m.ttlTicker = time.NewTicker(time.Duration(m.d)) // TODO: set ttl according to m.d

	m.compactFromPrevNotifier = make(chan struct{}, 1) // should be async, required buffer

	m.soCompactionTrigger = so
	m.sdCompactionTrigger = sd
	m.ddCompactionTrigger = dd

	ttlDaemonCtx, ttlDaemonCancel := context.WithCancel(context.Background())
	m.ttlDaemonCancel = ttlDaemonCancel
	go m.ttlDaemon(ttlDaemonCtx)

	return m
}

func (m *level) close() {
	m.ttlDaemonCancel()
	m.ttlTicker.Stop()
}

func (m *level) compactFromPrevNotify() {
	m.compactFromPrevNotifier <- struct{}{}
}

// TTL
func (m *level) ttlDaemon(ctx context.Context) {
	for {
		select {
		case <-m.compactFromPrevNotifier:
			// If there are compaction from the prev level, this level reset ttl ticker
			{
				m.ttlTicker.Reset(time.Duration(m.d))
			}
		case <-m.ttlTicker.C:
			// trigge compaction periodically aimed at enforcing a finite bound for delete persistence latency
			{
				// TODO
				m.ddCompactionTrigger <- compactionTask{}
			}
		case <-ctx.Done():
			{
				log.Printf("stop ttlDaemon from Level %d\n", m.levelID)
				return
			}
		}
	}
}

func (m *level) get(key []byte) (value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return nil
}

// paper 4.1.2
func (m *level) computeD() float64 {
	return 0.0
}

// paper 4.1.2
// updating d
func (m *level) updateD() {
	m.d = 0.0
}
