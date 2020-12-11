package examples

import (
	"fmt"
	"lethe"
	"testing"
)

func TestBasicUsage(t *testing.T) {
	c, err := lethe.NewCollection(lethe.DefaultCollectionOptions)
	if err != nil {
		fmt.Println(err)
	}

	// start a collection
	c.Start()
	defer c.Close()

	// wtire
	c.Put([]byte("car-0"), []byte("tesla"))
	c.Put([]byte("car-1"), []byte("honda"))

	// read
	ropts := lethe.ReadOptions{}
	val0, err := c.Get([]byte("car-0"), ropts)         // val0 == []byte("tesla").
	valX, err := c.Get([]byte("car-not-there"), ropts) // valX == nil.
	val1, err := c.Get([]byte("car-1"), ropts)         // val1 == []byte("honda").

	fmt.Println(string(val0), string(valX), string(val1))
}
