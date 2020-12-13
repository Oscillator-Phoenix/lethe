package tests

import (
	"lethe"
	"testing"
)

// TODO
func TestBasic1(t *testing.T) {
	c, err := lethe.NewCollection(lethe.DefaultCollectionOptions)
	if err != nil {
		t.Fatalf("NewCollection\n")
	}

	// start a collection
	c.Start()
	defer c.Close()

	batchSize := 10
	ks, vs := genBatchKV(batchSize)

	wopts := lethe.WriteOptions{}
	for i := 0; i < len(ks); i++ {
		if err = c.Put(ks[i], vs[i], wopts); err != nil {
			t.Fatalf("Put\n")
		}
	}

	ropts := lethe.ReadOptions{}
	getVs := [][]byte{}
	for i := 0; i < len(ks); i++ {
		v, err := c.Get(ks[i], ropts)
		if v == nil || err != nil {
			t.Fatalf("Get\n")
		}
		getVs = append(getVs, v)
	}

	if isEqualBatchBytes(vs, getVs) == false {
		t.Fatalf("The value got is different to the value put.\n")
	}
}
