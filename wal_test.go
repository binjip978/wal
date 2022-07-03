package wal

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
)

func BenchmarkWrite32(b *testing.B) {
	benchmarkWrite([]byte("0123456789ABCDEF0123456789ABCDEF"), b)
}

func benchmarkWrite(msg []byte, b *testing.B) {
	b.StopTimer()
	tempDir, _ := ioutil.TempDir("", "wal-bench")
	defer os.RemoveAll(tempDir)
	cfg := Config{}
	cfg.Segment.MaxIndexSizeBytes = 4 * 2 << 20
	cfg.Segment.MaxStoreSizeBytes = 16 * 2 << 20
	log, _ := New(tempDir, cfg)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _ = log.Append(msg)
	}
	b.StopTimer()

	log.Close()
}

func TestWalEmptyDir(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "wal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := New(tempDir, Config{})
	if err != nil {
		t.Fatal(err)
	}

	if tempDir != wal.dir {
		t.Error("dir is not correct")
	}

	if wal.activeSegment == nil {
		t.Error("active segment is nil")
	}

	// should have default name is dir empty
	_, err = os.Stat(tempDir + "/0001.index")
	if err != nil {
		t.Error("index file is missing")
	}

	_, err = os.Stat(tempDir + "/0001.store")
	if err != nil {
		t.Error("store file is missing")
	}
}

func TestWalReadWrite(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "wal-test-rw")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := New(tempDir, Config{})
	if err != nil {
		t.Fatal(err)
	}

	var ids []uint64
	messages := []string{
		"jello",
		"moon",
		"landing",
		"one string with space",
		`{"hello": 12, "world": [1, 2, "3"]}`,
	}

	for _, message := range messages {
		id, err := wal.Append([]byte(message))
		if err != nil {
			t.Error(err)
		}

		ids = append(ids, id)
	}

	for i, id := range ids {
		data, err := wal.Read(id)
		if err != nil {
			t.Error(err)
		}
		if string(data) != messages[i] {
			t.Error("message is the same")
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := defaultConfig(Config{})
	if cfg.Segment.MaxIndexSizeBytes != defaultIndexSize {
		t.Error("default value for index is not set")
	}

	if cfg.Segment.MaxStoreSizeBytes != defaultStoreSize {
		t.Error("default value for store is not set")
	}

	cfg = Config{}
	cfg.Segment.MaxIndexSizeBytes = 1000
	cfg.Segment.MaxStoreSizeBytes = 2000

	cfg = defaultConfig(cfg)
	if cfg.Segment.MaxIndexSizeBytes != 1000 {
		t.Error("should not change non zero value")
	}
	if cfg.Segment.MaxStoreSizeBytes != 2000 {
		t.Error("should not change non zero value")
	}
}

func TestConcurrentAppends(t *testing.T) {
	cfg := defaultConfig(Config{})
	cfg.Segment.MaxIndexSizeBytes = 1 * 2 << 20
	cfg.Segment.MaxStoreSizeBytes = 1 * 2 << 20
	tempDir, err := ioutil.TempDir("", "wal-conc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := New(tempDir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}

	appender := func(t *testing.T, w *WAL, id string, wg *sync.WaitGroup) {
		results := make(map[uint64][]byte)
		defer wg.Done()
		for i := 0; i < 5; i++ {
			payload := []byte(fmt.Sprintf("%s-%d", id, i))
			id, err := w.Append(payload)
			if err != nil {
				t.Error(err)
			}
			results[id] = payload
		}

		for k, v := range results {
			data, err := w.Read(k)
			if err != nil {
				t.Error(err)
			}
			if !bytes.Equal(v, data) {
				t.Error("no equal")
			}
		}
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go appender(t, wal, fmt.Sprintf("go-%d", i), wg)
	}

	wg.Wait()
}
