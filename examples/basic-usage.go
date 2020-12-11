package examples

import (
	"fmt"
	"lethe"
)

func main() {
	c, err := lethe.NewCollection(lethe.DefaultCollectionOptions)
	if err != nil {
		fmt.Println(err)
	}

	c.Start()
	defer c.Close()

	batch, err := c.NewBatch()
	defer batch.Close()

	batch.Set([]byte("car-0"), []byte("tesla"))
	batch.Set([]byte("car-1"), []byte("honda"))

	err = c.ExecuteBatch(batch, lethe.WriteOptions{})

	ss, err := c.Snapshot()
	defer ss.Close()

	ropts := lethe.ReadOptions{}

	val0, err := ss.Get([]byte("car-0"), ropts)         // val0 == []byte("tesla").
	valX, err := ss.Get([]byte("car-not-there"), ropts) // valX == nil.

	// A Get can also be issued directly against the collection
	val1, err := c.Get([]byte("car-1"), ropts) // val1 == []byte("honda").

	fmt.Println(val0, valX, val1)
}
