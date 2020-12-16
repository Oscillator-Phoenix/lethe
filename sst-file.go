package lethe

import (
	"sync"
	"time"
)

type sstFile struct {
	fileID int

	mu    sync.Mutex
	tiles []*deleteTile

	ttl time.Duration

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

	return m
}

func (m *sstFile) close() {
}

func (m *sstFile) updateTTL(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// update TTL of this file
	m.ttl = ttl
}
