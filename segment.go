package wal

import (
	"path/filepath"
	"strings"
)

type segment struct {
	idx       *index
	store     *store
	segmentID string
}

func (s *segment) read(id uint64) ([]byte, error) {
	offset, err := s.idx.read(id)
	if err != nil {
		return nil, err
	}

	data, err := s.store.read(offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *segment) write(data []byte) (uint64, error) {
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

func (s *segment) remove() error {
	err := s.idx.remove()
	if err != nil {
		return err
	}

	return s.store.remove()
}

func newSegment(indexFile string, storeFile string, startID uint64, cfg *Config) (*segment, error) {
	index, err := newIndex(indexFile, cfg, startID)
	if err != nil {
		return nil, err
	}

	store, err := newStore(storeFile, cfg)
	if err != nil {
		return nil, err
	}

	sp := strings.Split(filepath.Base(indexFile), ".")

	return &segment{
		idx:       index,
		store:     store,
		segmentID: sp[0],
	}, nil
}
