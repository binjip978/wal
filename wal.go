package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type WAL struct {
	dir           string
	activeSegment *segment
	segments      []*segment
	mu            sync.Mutex
	config        *Config
}

var (
	ErrRecordNotFound   = errors.New("record is not found")
	ErrNoStoreSpaceLeft = errors.New("no store space left")
	ErrNoIndexSpaceLeft = errors.New("no index space left")
)

// New creates a Write Ahead Log in specified directory
// it will look for files [d+].store and [d+].index
// if no such files present it will create empty them
// TODO: multiple segments should be supported
func New(dir string, cfg *Config) (*WAL, error) {
	var walConfig = Config{}
	if cfg == nil {
		walConfig = defaultConfig
	} else {
		walConfig = *cfg
	}

	fNames, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segments []*segment
	var startID uint64 = 1

	// TODO: sort files to determine last segment (active one)
	for _, fName := range fNames {
		if strings.Contains(fName.Name(), ".store") {
			sp := strings.Split(fName.Name(), ".")
			indexPath := filepath.Join(dir, sp[0]+".index")
			storePath := filepath.Join(dir, fName.Name())

			//read first 8 bytes from segment file
			iStat, err := os.Stat(indexPath)
			if err != nil {
				return nil, err
			}

			if iStat.Size() > 0 {
				// read first 8 bytes to get startID for segment
				f, err := os.Open(indexPath)
				if err != nil {
					return nil, err
				}
				b := make([]byte, 8)
				n, err := f.Read(b)
				if err != nil {
					return nil, err
				}
				if n != 8 {
					// TODO: should be an error
					panic("can't read 8 bytes")
				}

				// TODO: will not work because segments is not yet sorted
				// but should be sufficient for one segment
				startID = binary.BigEndian.Uint64(b)
			}

			segment, err := newSegment(indexPath, storePath, startID, &walConfig)
			if err != nil {
				return nil, fmt.Errorf("can't initiate segment: %w", err)
			}

			segments = append(segments, segment)

			// TODO: add support for multiple segments
			break
		}
	}

	// no segments are present starting new log
	if len(segments) == 0 {
		indexPath := filepath.Join(dir, "0001.index")
		f, err := os.Create(indexPath)
		if err != nil {
			return nil, err
		}
		_ = f.Close()

		storePath := filepath.Join(dir, "0001.store")
		f, err = os.Create(storePath)
		if err != nil {
			return nil, err
		}
		_ = f.Close()

		segment, err := newSegment(indexPath, storePath, 1, &walConfig)
		if err != nil {
			return nil, err
		}

		segments = append(segments, segment)
	}

	wal := &WAL{
		dir:           dir,
		activeSegment: segments[len(segments)-1],
		mu:            sync.Mutex{},
		segments:      segments,
		config:        &walConfig,
	}

	return wal, nil
}

func (w *WAL) Append(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	id, err := w.activeSegment.write(data)
	// no more space for index or store, create new one
	if errors.Is(err, ErrNoIndexSpaceLeft) || errors.Is(err, ErrNoStoreSpaceLeft) {
		nID := nextID(w.activeSegment.segmentID)
		indexF, err := os.Create(filepath.Join(w.dir, nID+".index"))
		if err != nil {
			return 0, err
		}
		storeF, err := os.Create(filepath.Join(w.dir, nID+".store"))
		if err != nil {
			return 0, err
		}
		_ = indexF.Close()
		_ = storeF.Close()

		nSeg, err := newSegment(indexF.Name(), storeF.Name(),
			w.activeSegment.idx.id, w.config)
		if err != nil {
			return 0, err
		}

		w.segments = append(w.segments, nSeg)
		w.activeSegment = nSeg

		id, err := w.activeSegment.write(data)
		if err != nil {
			return 0, err
		}

		return uint64(id), nil
	}
	if err != nil {
		return 0, err
	}

	return uint64(id), nil
}

func (w *WAL) Read(id uint64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if id < w.segments[0].idx.startID {
		return nil, ErrRecordNotFound
	}

	// determine correct segment
	for i := 0; i < len(w.segments)-1; i++ {
		ls := w.segments[i].idx.startID
		rs := w.segments[i+1].idx.startID

		if id >= ls && id < rs {
			data, err := w.segments[i].read(id)
			if err != nil {
				return nil, err
			}

			return data, nil
		}
	}

	data, err := w.activeSegment.read(id)
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

// Trim remove all segments that startID is less than id
func (w *WAL) Trim(id uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var newSegments []*segment

	for i := 0; i < len(w.segments)-1; i++ {
		if w.segments[i+1].idx.startID <= id {
			err := w.segments[i].remove()
			if err != nil {
				return err
			}

			continue
		}
		newSegments = append(newSegments, w.segments[i])
	}

	newSegments = append(newSegments, w.activeSegment)

	w.segments = newSegments
	return nil
}

func nextID(oldID string) string {
	skip := 0
	for i := 0; i < len(oldID); i++ {
		if oldID[i] != '0' {
			break
		}
		skip++
	}

	n, err := strconv.Atoi(oldID[skip:])
	if err != nil {
		panic("creating new index is broken")
	}

	newID := strconv.Itoa(n + 1)
	return strings.Repeat("0", 4-len(newID)) + newID
}
