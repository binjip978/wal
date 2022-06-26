package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type recordOffset uint64

// record defines unit of the log it store the record size and record
// data, recordID is stored in the index as a mapping recordID -> recordOffSet
type record struct {
	id   recordID
	data []byte
}

// store defines a storage abstraction for the log
// log is append only file
type store struct {
	file    *os.File
	mu      sync.Mutex
	size    uint64
	maxSize uint64
}

// newStore returns a new storage
func newStore(file string, cfg Config) (*store, error) {
	st, err := os.Stat(file)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(file, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &store{
		file:    f,
		mu:      sync.Mutex{},
		size:    uint64(st.Size()),
		maxSize: cfg.Segment.MaxStoreSizeBytes,
	}, nil
}

// read takes an offset in a file and returns a record
func (s *store) read(offset recordOffset) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// read the first 8 bytes to determine the size of the record
	b := make([]byte, 8)
	_, err := s.file.ReadAt(b, int64(offset))
	if err != nil {
		return nil, err
	}

	var size uint64
	err = binary.Read(bytes.NewReader(b), binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}

	b = make([]byte, size)
	_, err = s.file.ReadAt(b, int64(offset)+8)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// write append the record to the log and return
func (s *store) write(data []byte) (recordOffset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO: could we possible overflow?
	// what is maximum slice length?
	if s.size+uint64(len(data)+8) > s.maxSize {
		return 0, ErrNoStoreSpaceLeft
	}

	err := binary.Write(s.file, binary.BigEndian, uint64(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := s.file.Write(data)
	if err != nil {
		return 0, err
	}

	if n != len(data) {
		return 0, fmt.Errorf("can't write all data")
	}

	err = s.file.Sync()
	if err != nil {
		return 0, err
	}

	offset := recordOffset(s.size)
	s.size += uint64(n + 8)

	return offset, nil
}

func (s *store) close() error {
	s.mu.Lock()
	s.mu.Unlock()

	return s.file.Close()
}
