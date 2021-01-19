package lethe

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// skipList is an ordered key-value map which was proposed by the paper below:
// https://www.epaperpress.com/sortsearch/download/skipList.pdf

const (
	defaultSkipListMaxLevel    int     = 32
	defaultSkipListProbability float32 = 0.25
)

type skipListNode struct {
	key      []byte
	entity   *sortedMapEntity
	forwards [](*skipListNode) // length of `forwards` is the level of this node
}

// skipList is a sorted Key-Value map.
// skipList implements the sortedMap interface.
type skipList struct {
	head *skipListNode

	// the number of kv entries in the skipList
	_size int

	// configuration
	less        func(s, t []byte) bool // less returns whether key s is less than key t.
	maxLevel    int
	probability float32
}

// newSkipListNode return a skipListNode via copying data
func newSkipListNode(key []byte, entity *sortedMapEntity, level int) *skipListNode {

	var node skipListNode

	node.key = copyBytes(key)
	node.entity = copySortedMapEntity(entity)
	node.forwards = make([](*skipListNode), level)

	return &node
}

// newSkipList returns a skipList using default configuration.
func newSkipList(less func(s, t []byte) bool) *skipList {
	var sl skipList

	sl._size = 0
	sl.less = less

	sl.probability = defaultSkipListProbability
	sl.maxLevel = defaultSkipListMaxLevel
	sl.head = newSkipListNode(nil, nil, sl.maxLevel) // initialize head-node with maxLevel

	rand.Seed(time.Now().Unix()) // optional: reset random number seed

	return &sl
}

// newskipListWith returns a skipList using custom configuration.
func newSkipListWith(less func(s, t []byte) bool, probability float32, maxLevel int) *skipList {
	var sl skipList

	sl._size = 0

	sl.less = less

	sl.probability = probability
	sl.maxLevel = maxLevel

	sl.head = newSkipListNode(nil, nil, sl.maxLevel) // initialize head-node with maxLevel

	rand.Seed(time.Now().Unix()) // optional: reset random number seed

	return &sl
}

// randomLevel returns a random level according to configuration
func (sl *skipList) randomLevel() int {
	level := 1
	for rand.Float32() < sl.probability && level < sl.maxLevel {
		level++
	}
	return level
}

// Num returns the number of kv entries in the skipList
func (sl *skipList) Num() int {
	return sl._size
}

// Empty returns whether skipList is empty
func (sl *skipList) Empty() bool {
	return sl.Num() == 0
}

// Get returns the copy of value by key.
// If the key is not found, it returns (nil, false).
func (sl *skipList) Get(key []byte) (entity *sortedMapEntity, ok bool) {

	x := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for x.forwards[i] != nil && sl.less(x.forwards[i].key, key) {
			x = x.forwards[i] // skip
		}
	}

	x = x.forwards[0]

	if x != nil && bytes.Equal(x.key, key) {
		return x.entity, true
	}

	return nil, false
}

// Put inserts a kv entry into skipList.
func (sl *skipList) Put(key []byte, entity *sortedMapEntity) error {
	// fmt.Println("head", sl.head)
	// fmt.Printf("to insert: (%d, %d)\n", key, value)

	update := make([]*skipListNode, sl.maxLevel)
	x := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for x.forwards[i] != nil && sl.less(x.forwards[i].key, key) {
			x = x.forwards[i] // skip
		}
		update[i] = x
	}

	x = x.forwards[0]

	// fmt.Println("search done")

	// replace existing old value with the new value, then return
	if x != nil && bytes.Equal(x.key, key) {
		x.entity = copySortedMapEntity(entity) // overwrite
		return nil                             // insert succeeded
	}

	newNodeLevel := sl.randomLevel() // function `randomLevel` make sure `newNodeLevel < sl.maxLevel`
	// fmt.Println("newNodeLevel", newNodeLevel)

	newNode := newSkipListNode(key, entity, newNodeLevel)
	for i := 0; i < newNodeLevel; i++ {
		newNode.forwards[i] = update[i].forwards[i]
		update[i].forwards[i] = newNode
	}
	sl._size++

	// fmt.Println("inserted (", key, ", ", value, ")")
	return nil // insert succeeded
}

// Del the kv entry by key
// Del is in-place delete that is not needed in LSM.
func (sl *skipList) Del(key []byte) error {
	if sl.Empty() {
		return nil
	}

	update := make([]*skipListNode, sl.maxLevel)
	x := sl.head
	for i := sl.maxLevel - 1; i >= 0; i-- {
		for x.forwards[i] != nil && sl.less(x.forwards[i].key, key) {
			x = x.forwards[i] // skip
		}
		update[i] = x
	}

	x = x.forwards[0]

	if x != nil && bytes.Equal(x.key, key) {
		for i := 0; i < sl.maxLevel; i++ {
			if update[i].forwards[i] != x {
				return nil // level of x done
			}
			update[i].forwards[i] = x.forwards[i]
		}
		sl._size--
	}

	return nil
}

// Traverse traverses the skipList in order defined by lessFunc
func (sl *skipList) Traverse(operation func(key []byte, entity *sortedMapEntity)) {
	// itereate on level-0 which is a single linked list
	x := sl.head.forwards[0]
	for x != nil {
		operation(x.key, x.entity)
		x = x.forwards[0]
	}
}

func reverseStrings(ss []string) []string {
	i := 0
	j := len(ss) - 1
	for i < j {
		ss[i], ss[j] = ss[j], ss[i]
		i++
		j--
	}
	return ss
}

func (sl skipList) String() string {
	ss := []string{}

	for i := 0; i < sl.maxLevel; i++ {
		var levelBuf bytes.Buffer
		var b bytes.Buffer

		x := sl.head.forwards[i]
		for x != nil {
			fmt.Fprintf(&b, "%v -> ", x.key) // bytes to string
			x = x.forwards[i]
		}

		if b.Len() == 0 {
			break
		}

		fmt.Fprintf(&levelBuf, "level %d: head -> ", i+1)
		b.WriteTo(&levelBuf)
		fmt.Fprintf(&levelBuf, "nil")
		ss = append(ss, levelBuf.String())
	}

	return strings.Join(reverseStrings(ss), "\n")
}
