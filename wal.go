package wal

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type WAL struct {
	dir           string
	activeSegment *segment
	mu            sync.Mutex
}

const (
	defaultStoreSize = 1024
	defaultIndexSize = 1024
)

var (
	ErrRecordNotFound   = errors.New("record is not found")
	ErrNoStoreSpaceLeft = errors.New("no store space left")
	ErrNoIndexSpaceLeft = errors.New("no index space left")
)

// New creates a Write Ahead Log in specified directory
// it will look for files [d+].store and [d+].index
// if no such files present it will create empty them
// TODO: multiple segments should be supported
func New(dir string, cfg Config) (*WAL, error) {
	cfg = configDefautls(cfg)

	fmt.Println(cfg)

	fNames, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segments []*segment

	// TODO: sort files to determine last segment (active one)
	for _, fName := range fNames {
		if strings.Contains(fName.Name(), ".store") {
			sp := strings.Split(fName.Name(), ".")
			indexPath := filepath.Join(dir, sp[0]+".index")
			storePath := filepath.Join(dir, fName.Name())

			segment, err := newSegment(indexPath, storePath, cfg)
			if err != nil {
				return nil, fmt.Errorf("can't initiate segment: %w", err)
			}

			segments = append(segments, segment)

			// TODO: add support for multiple segments
			break
		}
	}

	if len(segments) == 0 {
		indexPath := filepath.Join(dir, "0001.index")
		f, err := os.Create(indexPath)
		if err != nil {
			return nil, err
		}
		f.Close()

		storePath := filepath.Join(dir, "0001.store")
		f, err = os.Create(storePath)
		if err != nil {
			return nil, err
		}
		f.Close()

		segment, err := newSegment(indexPath, storePath, cfg)
		if err != nil {
			return nil, err
		}

		segments = append(segments, segment)
	}

	wal := &WAL{
		dir:           dir,
		activeSegment: segments[len(segments)-1],
		mu:            sync.Mutex{},
	}

	return wal, nil
}

func (w *WAL) Append(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset, err := w.activeSegment.store.write(data)
	if err != nil {
		return 0, err
	}

	id, err := w.activeSegment.idx.write(offset)
	if err != nil {
		return 0, err
	}

	return uint64(id), nil
}

func (w *WAL) Read(id uint64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset, err := w.activeSegment.idx.read(recordID(id))
	if err != nil {
		return nil, err
	}

	data, err := w.activeSegment.store.read(offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (w *WAL) Close() error {
	err := w.activeSegment.idx.close()
	if err != nil {
		return err
	}

	err = w.activeSegment.store.close()
	if err != nil {
		return err
	}

	return nil
}

func configDefautls(cfg Config) Config {
	if cfg.Segment.MaxIndexSizeBytes == 0 {
		cfg.Segment.MaxIndexSizeBytes = defaultIndexSize
	}
	if cfg.Segment.MaxStoreSizeBytes == 0 {
		cfg.Segment.MaxStoreSizeBytes = defaultStoreSize
	}

	return cfg
}