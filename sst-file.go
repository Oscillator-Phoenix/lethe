package lethe

type sstFile struct {
	tiles []deleteTile

	primaryKeyMin []byte
	primaryMax    []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte
}

func (m *sstFile) get(key []byte) (value []byte) {
	return nil
}
