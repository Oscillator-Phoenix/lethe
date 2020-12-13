package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// skipList is an ordered key-value map which was proposed by the paper below:
// https://www.epaperpress.com/sortsearch/download/skiplist.pdf

const (
	defaultMaxLevel    int     = 32
	defaultProbability float32 = 0.25
)

// LessFunc returns whether key s is less than key t.
type LessFunc func(s, t []byte) bool

type keyValue struct {
	key   []byte
	value []byte
}

type skipListNode struct {
	keyValue
	forwards [](*skipListNode) // length of `forwards` is the level of this node
}

// SkipList is a sorted Key-Value map.
type SkipList struct {
	head *skipListNode

	// the number of kv entries in the skiplist
	_size int

	// configuration
	less        LessFunc
	equal       LessFunc
	maxLevel    int
	probability float32
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// newSkipListNode return a skipListNode via copying data
func newSkipListNode(key, value []byte, level int) *skipListNode {
	var node skipListNode
	node.key = copyBytes(key)
	node.value = copyBytes(value)
	node.forwards = make([](*skipListNode), level)
	return &node
}

// NewSkipList returns a skiplist using default configuration.
func NewSkipList(less func(s, t []byte) bool) SkipList {
	var sl SkipList

	sl._size = 0
	sl.less = less
	sl.equal = bytes.Equal

	sl.probability = defaultProbability
	sl.maxLevel = defaultMaxLevel
	sl.head = newSkipListNode(nil, nil, sl.maxLevel) // initialize head-node with maxLevel

	rand.Seed(time.Now().Unix()) // optional: reset random number seed

	return sl
}

// NewSkipListWith returns a skiplist using custom configuration.
func NewSkipListWith(less func(s, t []byte) bool, probability float32, maxLevel int) SkipList {
	var sl SkipList

	sl._size = 0
	sl.less = less
	sl.equal = bytes.Equal

	sl.probability = probability
	sl.maxLevel = maxLevel
	sl.head = newSkipListNode(nil, nil, sl.maxLevel) // initialize head-node with maxLevel

	rand.Seed(time.Now().Unix()) // optional: reset random number seed

	return sl
}

// randomLevel returns a random level according to configuration
func (sl *SkipList) randomLevel() int {
	level := 1
	for rand.Float32() < sl.probability && level < sl.maxLevel {
		level++
	}
	return level
}

// Size returns the number of kv entries in the skiplist
func (sl *SkipList) Size() int {
	return sl._size
}

// Empty returns whether skiplist is empty
func (sl *SkipList) Empty() bool {
	return sl.Size() == 0
}

// Get returns the copy of value by key.
// If the key is not found, it returns (nil, false).
func (sl *SkipList) Get(key []byte) (value []byte, ok bool) {
	x := sl.head
	for i := sl.maxLevel - 1; i >= 0; i-- {
		for x.forwards[i] != nil && sl.less(x.forwards[i].key, key) {
			x = x.forwards[i] // skip
		}
	}
	x = x.forwards[0]
	if x != nil && sl.equal(x.key, key) {
		return copyBytes(x.value), true
	}
	return nil, false
}

// Put inserts a kv entry into skiplist.
func (sl *SkipList) Put(key, value []byte) error {
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
	if x != nil && sl.equal(x.key, key) {
		x.value = copyBytes(value)
		return nil // insert succeeded
	}

	newNodeLevel := sl.randomLevel() // function `randomLevel` make sure `newNodeLevel < sl.maxLevel`
	// fmt.Println("newNodeLevel", newNodeLevel)

	newNode := newSkipListNode(key, value, newNodeLevel)
	for i := 0; i < newNodeLevel; i++ {
		newNode.forwards[i] = update[i].forwards[i]
		update[i].forwards[i] = newNode
	}
	sl._size++

	// fmt.Println("inserted (", key, ", ", value, ")")
	return nil // insert succeeded
}

// Del the kv entry by key
func (sl *SkipList) Del(key []byte) error {
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

	if x != nil && sl.equal(x.key, key) {
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

// Traverse traverses the skipList in the order defined by lessFunc
func (sl *SkipList) Traverse(operate func(key, value []byte)) {
	// itereate on level-0 which is a single linked list
	x := sl.head.forwards[0]
	for x != nil {
		operate(x.key, x.value)
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

func (sl SkipList) String() string {
	ss := []string{}

	for i := 0; i < sl.maxLevel; i++ {
		var levelBuf bytes.Buffer
		var b bytes.Buffer

		x := sl.head.forwards[i]
		for x != nil {
			fmt.Fprintf(&b, "%s -> ", string(x.key)) // bytes to string
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
