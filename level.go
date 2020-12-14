package lethe

import "sync"

// Key Weaving Storage Layout
// - files contain delete tiles, delete tiles within a file are sorted on primary key
// - delete-tiles contain pages, pages within a delete tile are sorted on delete key
// - entries within every page are sorted on primary key

type level struct {
	mu    sync.Mutex
	files []sstFile
}

func (m *level) get(key []byte) (value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return nil
}
