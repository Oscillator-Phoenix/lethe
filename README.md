# lethe

lethe is a LSM-based KV database which is a reproduction of the paper [Lethe: A Tunable Delete-Aware LSM Engine](https://dl.acm.org/doi/10.1145/3318464.3389757).

<div align="center">
  [![GitHub license](https://img.shields.io/github/license/mashape/apistatus.svg?style=flat-square)](https://github.com/Oscillator-Phoenix/lethe/blob/master/LICENSE)
</div>

---

## 1. For Users

### 1.1 How to use lethe

basic usage like below:

```go
// TODO
```

Further, see [examples](./examples).

---

### 1.2 How to configurate lethe

todo

---

## 2. For Developers

### 2.1 Regular Task

1. maintain [tests](./tests) following lastest modification

2. maintain [examples](./examples) following lastest version

3. maintain [doc](./doc) following lastest version

---

### 2.2 TODO Task

1. encode in-memory LSM data structre to disk and rebuild in-memory LSM data structure form disk (Medium, Basic)
2. change linear search code to binary search code if possible(Medium, Basic)
3. change `sync.Mutex` to `sync.RWMutex` if possible. (Easy, Performance)
4. support write option: sync write. (Easy, Usability)
5. add parallel compaction. (Hard, Performance)
6. Write Ahead Log: atomic, recovery. (Hard, Performance, Usability)
7. add read-only `snapshot`: MVCC. (Hard, Performance, Usability)
8. add actomic wirte `batch`: **Depend on 4**. (Medium, Usability)
9. support config file, using [toml](https://pkg.go.dev/github.com/BurntSushi/toml) format. (Medium, Usability)
10. support cli of lethe. (Medium, Usability)
11. use `sync.Pool` in [memTable](./memtable.go) to reduce the times of memory allocation (Medium, Performance)

---

### 2.3 Bug Fix Task

As far, with little test, no bug found.

---

## 3. References

### 3.1 Papers

- [Lethe: A Tunable Delete-Aware LSM Engine](https://dl.acm.org/doi/10.1145/3318464.3389757)

### 3.2 Projects

- https://github.com/facebook/rocksdb
- https://github.com/syndtr/goleveldb
- https://github.com/couchbase/moss
- https://github.com/boltdb/bolt
