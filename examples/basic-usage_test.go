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
	defer c.Close()

	// wtire
	wopts := &lethe.WriteOptions{}
	c.Put([]byte("car-0"), []byte("tesla"), nil, wopts)
	c.Put([]byte("car-1"), []byte("honda"), nil, wopts)

	// read
	ropts := &lethe.ReadOptions{}
	val0, err := c.Get([]byte("car-0"), ropts)         // val0 == []byte("tesla").
	valX, err := c.Get([]byte("car-not-there"), ropts) // valX == nil.
	val1, err := c.Get([]byte("car-1"), ropts)         // val1 == []byte("honda").

	fmt.Println(string(val0) == "tesla", string(valX) == "", string(val1) == "honda")
	if !(string(val0) == "tesla" && string(valX) == "" && string(val1) == "honda") {
		t.Fail()
	}
}
