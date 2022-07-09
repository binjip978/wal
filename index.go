package wal

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/edsrzf/mmap-go"
)

var ErrMaxIndexSize = errors.New("max index size should be multiple by 16 and more than 0")

// index will store mapping between recordID and recordOffset
// it will maintain it in memory and in index file
type index struct {
	mm      mmap.MMap
	idxFile *os.File
	maxSize uint64
	size    uint64
	id      uint64
	startID uint64
}

func (i *index) write(offset uint64) (uint64, error) {
	ii := (i.id - i.startID) * 16

	if ii >= i.maxSize {
		return 0, errNoIndexSpaceLeft
	}

	binary.BigEndian.PutUint64(i.mm[ii:ii+8], i.id)
	binary.BigEndian.PutUint64(i.mm[ii+8:ii+16], offset)

	i.size += 16
	i.id++

	return i.id - 1, i.mm.Flush()
}

func (i *index) read(id uint64) (uint64, error) {
	ii := (id - i.startID) * 16
	if id == 0 || ii >= i.size {
		return 0, ErrRecordNotFound
	}

	sID := binary.BigEndian.Uint64(i.mm[ii : ii+8])
	sOffset := binary.BigEndian.Uint64(i.mm[ii+8 : ii+16])

	if sID != id {
		panic("write or read is not working correctly")
	}

	return sOffset, nil
}

func (i *index) close() error {
	return i.idxFile.Close()
}

func (i *index) remove() error {
	return os.Remove(i.idxFile.Name())
}

func newIndex(file string, cfg *Config, startID uint64) (*index, error) {
	if startID == 0 {
		panic("recordID should not be zero")
	}

	if cfg.Segment.MaxIndexSizeBytes == 0 || cfg.Segment.MaxIndexSizeBytes%16 != 0 {
		return nil, ErrMaxIndexSize
	}

	_, err := os.Stat(file)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(file, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	err = os.Truncate(f.Name(), int64(cfg.Segment.MaxIndexSizeBytes))
	if err != nil {
		return nil, err
	}

	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	var size uint64
	id := startID
	// id is 1 to be sure that we never see tuple (0, 0) in index
	// that actually is real index

	// size is a byte offset of the index file
	// all new writes should go mm[size:size+16]
	// and size += 16
	// TODO: better name maybe?
	for i := 0; i < len(mm); i += 16 {
		b1 := binary.BigEndian.Uint64(mm[i : i+8])
		b2 := binary.BigEndian.Uint64(mm[i+8 : i+16])

		if b1 == 0 && b2 == 0 {
			break
		}

		size += 16
		id++
	}

	idx := &index{
		mm:      mm,
		idxFile: f,
		maxSize: cfg.Segment.MaxIndexSizeBytes,
		size:    size,
		id:      id,
		startID: startID,
	}

	return idx, nil
}
