package wal

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"sync"
)

type recordID uint64

type indexRecord struct {
	id     recordID
	offset recordOffset
}

// index will store mapping between recordID and recordOffset
// it will maintain it in memory and in index file
type index struct {
	mu      sync.Mutex
	mmap    map[recordID]recordOffset
	f       *os.File
	lastID  recordID
	size    uint64
	maxSize uint64
}

func (i *index) write(offset recordOffset) (recordID, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size+16 > i.maxSize {
		return 0, ErrNoIndexSpaceLeft
	}

	i.lastID++
	ir := indexRecord{id: i.lastID, offset: offset}

	err := binary.Write(i.f, binary.BigEndian, ir.id)
	if err != nil {
		return 0, err
	}

	err = binary.Write(i.f, binary.BigEndian, ir.offset)
	if err != nil {
		return 0, err
	}

	i.size += 16

	i.mmap[ir.id] = ir.offset
	return i.lastID, i.f.Sync()
}

func (i *index) read(id recordID) (recordOffset, error) {
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

func newIndex(file string, cfg Config) (*index, error) {
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
	mmap := make(map[recordID]recordOffset)
	var i int
	var lastID recordID

	for j := 0; j < nRecords; j++ {
		var id recordID
		var offset recordOffset

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
		lastID:  lastID,
		size:    uint64(len(b)),
		maxSize: cfg.Segment.MaxIndexSizeBytes,
	}, nil
}
