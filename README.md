# lethe

lethe is a LSM-based KV database which is a reproduction of the paper [Lethe: A Tunable Delete-Aware LSM Engine](https://dl.acm.org/doi/10.1145/3318464.3389757).

---

## 1. For Users

### 1.1 How to use lethe

basic usage like below:

```go
c, err := lethe.NewCollection(lethe.DefaultCollectionOptions)
if err != nil {
    fmt.Println(err)
}

// start a collection
c.Start()
defer c.Close()

// wtire
wopts := &lethe.WriteOptions{}
c.Put([]byte("car-0"), []byte("tesla"), wopts)
c.Put([]byte("car-1"), []byte("honda"), wopts)

// read
ropts := &lethe.ReadOptions{}
val0, err := c.Get([]byte("car-0"), ropts)         // val0 == []byte("tesla").
valX, err := c.Get([]byte("car-not-there"), ropts) // valX == nil.
val1, err := c.Get([]byte("car-1"), ropts)         // val1 == []byte("honda").

```

Further, see [examples](./examples).

---

### 1.2 How to configurate lethe

todo

---

## 2. For Developers

### 2.1 Regular Task

1. maintain [examples](./examples) following lastest version

2. maintain [doc](./doc) following lastest version

---

### 2.2 TODO Task

1. (Simple) change `sync.Mutex` to `sync.RWMutex` if possible.
2. (Simple) add write option: sync write.
3. (Hard) add parallel compaction.
4. (Hard) Write Ahead Log: atomic, recovery
5. (Hard) add read-only `snapshot` feature: MVCC.
6. (Medium) add actomic wirte `batch`. **Depend on 4**

---

### 2.3 Bug Fix Task

As far, no bug found.

---
