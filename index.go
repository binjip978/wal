package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"sync"
)

var ErrMaxIndexSize = errors.New("max index size should be multiple by 16")

// index will store mapping between recordID and recordOffset
// it will maintain it in memory and in index file
type index struct {
	mu      sync.Mutex
	mmap    map[uint64]uint64
	f       *os.File
	size    uint64
	maxSize uint64
	id      uint64
}

func (i *index) write(offset uint64) (uint64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size+16 > i.maxSize {
		return 0, ErrNoIndexSpaceLeft
	}

	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(i.id))
	binary.BigEndian.PutUint64(b[8:16], uint64(offset))

	_, err := i.f.Write(b)
	if err != nil {
		return 0, err
	}

	i.mmap[i.id] = offset
	i.size += 16
	i.id++

	return i.id - 1, i.f.Sync()
}

func (i *index) read(id uint64) (uint64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	offset, ok := i.mmap[id]
	if !ok {
		return 0, ErrRecordNotFound
	}

	return offset, nil
}

func (i *index) close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.f.Close()
}

func (i *index) remove() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	return os.Remove(i.f.Name())
}

func newIndex(file string, cfg Config) (*index, error) {
	if cfg.Segment.MaxIndexSizeBytes%16 != 0 {
		return nil, ErrMaxIndexSize
	}

	_, err := os.Stat(file)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(file, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	nRecords := len(b) / 16
	mmap := make(map[uint64]uint64)
	var i int
	var lastID uint64

	for j := 0; j < nRecords; j++ {
		var id uint64
		var offset uint64

		err := binary.Read(bytes.NewReader(b[i:i+8]),
			binary.BigEndian, &id)
		if err != nil {
			return nil, err
		}

		err = binary.Read(bytes.NewReader(b[i+8:i+16]),
			binary.BigEndian, &offset)
		if err != nil {
			return nil, err
		}

		mmap[id] = offset
		i += 16
		lastID = id // they should be in order
	}

	return &index{
		mu:      sync.Mutex{},
		mmap:    mmap,
		f:       f,
		id:      lastID,
		size:    uint64(len(b)),
		maxSize: cfg.Segment.MaxIndexSizeBytes,
	}, nil
}
