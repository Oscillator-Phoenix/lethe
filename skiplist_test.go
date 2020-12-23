package lethe

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

var (
	stringLessFunc = func(s, t []byte) bool { return string(s) < string(t) }
	intLessFunc    = func(s, t []byte) bool { return bytesToInt(s) < bytesToInt(t) }
)

func intToBytes(n int) []byte {
	data := int64(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

func bytesToInt(bys []byte) int {
	bytebuff := bytes.NewBuffer(bys)
	var data int64
	binary.Read(bytebuff, binary.BigEndian, &data)
	return int(data)
}

func TestRandomLevel(t *testing.T) {
	// TODO
}

func simpleExample() *skipList {
	sl := newSkipList(intLessFunc)
	simpleExamplePut(sl)
	return sl
}

func simpleExampleWith(probability float32, maxLevel int) *skipList {
	sl := newSkipListWith(intLessFunc, probability, maxLevel)
	simpleExamplePut(sl)
	return sl
}

func simpleExamplePut(sl *skipList) {
	xs := []int{3, 6, 7, 9, 12, 17, 19, 21, 25, 26}
	rand.Shuffle(len(xs), func(i, j int) {
		xs[i], xs[j] = xs[j], xs[i]
	})

	for i := 0; i < len(xs); i++ {
		k := intToBytes(xs[i])
		v := intToBytes(xs[i])
		e := &sortedMapEntity{}
		e.value = v
		if err := sl.Put(k, e); err != nil {
			panic("Put fialed")
		}
	}
}

func TestPut1(t *testing.T) {
	sl := newSkipList(intLessFunc)
	// fmt.Println(sl)

	xs := []int{3, 6, 7, 9, 12, 17, 19, 21, 25, 26}
	rand.Shuffle(len(xs), func(i, j int) {
		xs[i], xs[j] = xs[j], xs[i]
	})
	// fmt.Println("Shuffled xs: ", xs)

	for i := 0; i < len(xs); i++ {
		k := intToBytes(xs[i])
		v := intToBytes(xs[i])
		e := &sortedMapEntity{}
		e.value = v
		if err := sl.Put(k, e); err != nil {
			t.Logf("faild at insert element xs[%d]=%s", i, string(k))
			t.Log("xs", xs)
			t.Fail()
		}
	}

	ys := []int{}
	sl.Traverse(func(key []byte, entity *sortedMapEntity) {
		// fmt.Printf("(%d, %d) ", key, value)
		ys = append(ys, bytesToInt(entity.value))
	})

	sort.Ints(xs)
	for i := 0; i < len(xs); i++ {
		if xs[i] != ys[i] {
			t.Fail()
		}
	}
}

func TestInsert2(t *testing.T) {
	build := func(xs []int, sl *skipList) {
		for i := 0; i < len(xs); i++ {
			k := intToBytes(xs[i])
			v := intToBytes(xs[i])
			e := &sortedMapEntity{}
			e.value = v
			if err := sl.Put(k, e); err != nil {
				t.Logf("faild at insert element xs[%d]=%d", i, xs[i])
				t.Fail()
			}
		}
	}

	check := func(xs []int, sl *skipList) {
		ys := []int{}
		sl.Traverse(func(key []byte, entity *sortedMapEntity) {
			ys = append(ys, bytesToInt(entity.value))
		})
		sort.Ints(xs)
		for i := 0; i < len(xs); i++ {
			if xs[i] != ys[i] {
				t.Fail()
			}
		}
	}

	const (
		testTimes      = 100
		testScaleLimit = 10000
		testNumRange   = 100000
	)

	newRandomInts := func(size int) []int {
		xs := map[int](struct{}){}
		for i := 0; i < size; i++ {
			xs[rand.Intn(testNumRange)] = struct{}{}
		}
		_xs := []int{}
		for x := range xs {
			_xs = append(_xs, x)
		}
		return _xs
	}

	newRandomScale := func(size int) []int {
		randtScale := newRandomInts(size)
		for i := 0; i < len(randtScale); i++ {
			randtScale[i] = randtScale[i] % testScaleLimit
		}
		return randtScale
	}

	scales := newRandomScale(testTimes)
	for i := 0; i < len(scales); i++ {
		xs := newRandomInts(scales[i])
		sl := newSkipList(intLessFunc)
		build(xs, sl)
		check(xs, sl)

		if (i+1)%5 == 0 {
			fmt.Printf("tests %d / %d (size: %d) passed\n", i+1, len(scales), len(xs))
		}
	}
}

func TestSkipListString(t *testing.T) {
	sl1 := simpleExample()
	fmt.Println(sl1)

	fmt.Println()

	sl2 := simpleExampleWith(0.4, 32)
	fmt.Println(sl2)
}
