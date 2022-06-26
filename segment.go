package wal

import (
	"sync"
)

type segment struct {
	mu    sync.Mutex
	idx   *index
	store *store
}

func (s *segment) read(id recordID) (record, error) {
	offset, err := s.idx.read(id)
	if err != nil {
		return record{}, nil
	}

	b, err := s.store.read(offset)
	if err != nil {
		return record{}, nil
	}

	return record{id: id, data: b}, nil
}

func (s *segment) write(data []byte) (recordID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset, err := s.store.write(data)
	if err != nil {
		return 0, err
	}

	id, err := s.idx.write(offset)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (s *segment) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.idx.close()
	if err != nil {
		return err
	}

	err = s.store.close()
	if err != nil {
		return err
	}

	return nil
}

func newSegment(indexFile string, storeFile string, cfg Config) (*segment, error) {
	index, err := newIndex(indexFile, cfg)
	if err != nil {
		return nil, err
	}

	store, err := newStore(storeFile, cfg)
	if err != nil {
		return nil, err
	}

	return &segment{
		mu:    sync.Mutex{},
		idx:   index,
		store: store,
	}, nil
}
