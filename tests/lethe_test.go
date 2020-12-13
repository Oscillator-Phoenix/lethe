package tests

import (
	"fmt"
	"lethe"
	"testing"
)

func TestBasic1(t *testing.T) {
	c, err := lethe.NewCollection(lethe.DefaultCollectionOptions)
	if err != nil {
		t.Fatalf("NewCollection\n")
	}

	// start a collection
	c.Start()
	defer c.Close()

	batchSize := 1000 * 100
	fmt.Println("genBatchKVA...")
	ks, vs, as := genBatchKVA(batchSize)

	ropts := &lethe.ReadOptions{}
	wopts := &lethe.WriteOptions{}

	// Get before Put
	for i := 0; i < batchSize; i++ {
		v, err := c.Get(ks[i], ropts)
		if v != nil {
			t.Fatalf("Get\n")
		}
		if err != lethe.ErrKeyNotFound {
			t.Fatalf("Get\n")
		}
	}

	// Put
	for i := 0; i < batchSize; i++ {
		if err = c.Put(ks[i], vs[i], wopts); err != nil {
			t.Fatalf("Put\n")
		}
		// t.Logf("Put [%s] [%s]\n", string(ks[i]), string(vs[i]))
	}

	// Get After Put
	for i := 0; i < batchSize; i++ {
		v, err := c.Get(ks[i], ropts)
		if err != nil {
			t.Fatalf("[%d/%d] Get %v\n", err, i, len(ks))
		}
		if v == nil {
			t.Fatalf("[%d/%d] Get failed\n", i, len(ks))
		}
		if !isEqualOneBytes(v, as[i]) {
			t.Logf("got %v\n", v)
			t.Logf("expected %v\n", as[i])
			t.Fatalf("[%d/%d] key[%s] got[%s], expected[%s].\n", i, len(ks), string(ks[i]), string(v), string(as[i]))
		}
		if (i+1)%(batchSize/20) == 0 {
			fmt.Printf("tests %d / %d passed\n", i+1, batchSize)
		}
	}

	// Del
	for i := 0; i < batchSize; i++ {
		if err := c.Del(ks[i], wopts); err != nil {
			t.Fatalf("Del: %v\n", err)
		}
	}

	// Get After Del
	for i := 0; i < batchSize; i++ {
		v, err := c.Get(ks[i], ropts)
		if v != nil {
			t.Logf("%v\n", v)
			t.Fatalf("Get: expected nil\n")
		}
		if err != lethe.ErrKeyNotFound {
			t.Fatalf("Get: expected %v\n", lethe.ErrKeyNotFound)
		}
	}
}
