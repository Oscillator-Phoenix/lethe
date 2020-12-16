package lethe

type deleteTile struct {
	pages []*page

	primaryKeyMin []byte
	primaryKeyMax []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte
}
