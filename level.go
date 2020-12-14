package lethe

// Key Weaving Storage Layout
// - files contain delete tiles, delete tiles within a file are sorted on primary key
// - delete-tiles contain pages, pages within a delete tile are sorted on delete key
// - entries within every page are sorted on primary key

type level struct {
	files []sstFile
}

func (m *level) get(key []byte) (value []byte) {
	return nil
}
