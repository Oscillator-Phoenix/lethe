package lethe

import (
	"context"
	"log"
	"sync"
	"time"
)

type sstFile struct {
	fileID  int
	levelID int

	mu    sync.Mutex
	tiles []deleteTile

	ttl       time.Duration
	ttlTicker *time.Ticker

	fileTTLDaemonCancel context.CancelFunc

	// metadata
	primaryKeyMin []byte
	primaryKeyMax []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte

	aMAX float64
	b    int
}

func newSSTFile(fileID int, ttl time.Duration) *sstFile {
	m := &sstFile{}

	m.fileID = fileID
	m.ttl = ttl
	m.ttlTicker = time.NewTicker(m.ttl)

	ctx, cancel := context.WithCancel(context.Background())
	m.fileTTLDaemonCancel = cancel
	go m.fileTTLDaemon(ctx)

	return m
}

func (m *sstFile) fileTTLDaemon(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			{
				log.Printf("Stop fileTTLDaemon from (level %d, file %d)\n", m.levelID, m.fileID)
				return
			}
		}
	}
}

func (m *sstFile) close() {
	m.fileTTLDaemonCancel()
	m.ttlTicker.Stop()
}

func (m *sstFile) get(key []byte) (value []byte) {

	// if key is not in the range of this file (i.e. primaryKeyMin <= key <= primaryKeyMax)
	if Less(key, m.primaryKeyMin) || Less(m.primaryKeyMax, key)) {
		return nil
	}

	// There are no repeated key in a sstFile.
	// If key is not found in this delete tile, found at next delete tile.
	// TODO: delete tile in a file are ordered by primary key, so here can use binary search
	for i := 0; i < len(m.tiles); i++ {
		if value := tiles.get(key); value != nil {
			return value
		}
	}

	return nil
}

// paper 4.1.3
// TODO
// Can the type of returned value be time.Duration ?
// computing a_max
func (m *sstFile) initAMAX() float64 {
	return 0.0
}

// paper 4.1.3
// TODO
// computing b
func (m *sstFile) computeB() int64 {
	return 0
}

// paper 4.1.3
// TODO
// updating a_max
func (m *sstFile) updateAMAX() {
	m.aMAX = 0.0
}

// paper 4.1.3
// TODO
// updating b
func (m *sstFile) updateB() {
	m.b = 0
}
