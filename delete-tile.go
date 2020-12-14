package lethe

type deleteTile struct {
	pages []page

	primaryKeyMin []byte
	primaryMax    []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte
}

func (m *deleteTile) get(key []byte) (value []byte) {
	return nil
}
