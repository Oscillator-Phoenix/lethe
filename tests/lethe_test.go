package tests

import (
	"fmt"
	"lethe"
	"log"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
}

func TestGetPutDelSerial(t *testing.T) {

	copts := lethe.DefaultCollectionOptions
	copts.MemTableSizeLimit = 4 << 20
	// copts.MemTableSizeLimit = 1 * 1024
	// copts.MemTableSizeLimit = 32

	c, err := lethe.NewCollection(copts)
	if err != nil {
		t.Fatal("NewCollection\n")
	}
	defer c.Close()

	batchSize := 2 * 1000 * 1000
	// batchSize := 1000
	// batchSize := 10

	fmt.Println("genBatchKVA...")
	ks, vs, as := genBatchKVA(batchSize)
	fmt.Println("genBatchKVA done")

	ropts := &lethe.ReadOptions{}
	wopts := &lethe.WriteOptions{}

	// Get before Put
	fmt.Println("Get before Put ...")
	for i := 0; i < batchSize; i++ {
		v, err := c.Get(ks[i], ropts)
		if v != nil && err != lethe.ErrKeyNotFound {
			t.Fatalf("Get\n")
		}
	}
	fmt.Println("Get before Put done")

	// Put
	fmt.Println("Put...")
	for i := 0; i < batchSize; i++ {
		if err = c.Put(ks[i], vs[i], []byte("..."), wopts); err != nil {
			t.Fatal("Put\n")
		}
		// t.Logf("Put [%s] [%s]\n", string(ks[i]), string(vs[i]))
	}
	fmt.Println("Put done")

	fmt.Printf("\nWaiting...\n\n")
	time.Sleep(1 * time.Second)

	// Get After Put
	fmt.Println("Get After Put ...")
	for i := 0; i < batchSize; i++ {
		v, err := c.Get(ks[i], ropts)
		if err != nil {
			t.Fatalf("[%d/%d] Get %v\n", i, len(ks), err)
		}
		if v == nil {
			t.Fatalf("[%d/%d] Get failed\n", i, len(ks))
		}
		if !isEqualOneBytes(v, as[i]) {
			t.Logf("got %v\n", v)
			t.Logf("expected %v\n", as[i])
			t.Fatalf("[%d/%d] key[%s] got[%s], expected[%s].\n", i, len(ks), string(ks[i]), string(v), string(as[i]))
		}
		if (i+1)%(batchSize/10) == 0 {
			fmt.Printf("tests %d / %d passed\n", i+1, batchSize)
		}
	}
	fmt.Println("Get After Put done")

	// Del
	fmt.Println("Del ...")
	for i := 0; i < batchSize; i++ {
		if err := c.Del(ks[i], wopts); err != nil {
			t.Fatalf("Del: %v\n", err)
		}
	}
	fmt.Println("Del done")

	// Get After Del
	fmt.Println("Get After Del ...")
	for i := 0; i < batchSize; i++ {
		v, err := c.Get(ks[i], ropts)
		if v != nil && err != lethe.ErrKeyNotFound {
			t.Logf("%v\n", v)
			t.Fatalf("Get: expected nil and %v\n", lethe.ErrKeyNotFound)
		}
	}
	fmt.Println("Get After Del done")
}

func TestGetPutDelConcurrent(t *testing.T) {
	// TODO
}
