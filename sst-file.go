package lethe

type sstFile struct {
	tiles []deleteTile

	primaryKeyMin []byte
	primaryMax    []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte

	aMAX float64
	b    int
}

func (m *sstFile) get(key []byte) (value []byte) {
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
