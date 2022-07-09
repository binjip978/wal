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
	log, _ := New(tempDir, &cfg)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _ = log.Append(msg)
	}
	b.StopTimer()

	_ = log.Close()
}

func TestWalEmptyDir(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "wal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := New(tempDir, nil)
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

	wal, err := New(tempDir, nil)
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

	// close and reread
	err = wal.Close()
	if err != nil {
		t.Error(err)
	}

	wal, err = New(tempDir, nil)
	if err != nil {
		t.Error(err)
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

func TestConcurrentAppends(t *testing.T) {
	cfg := Config{}
	cfg.Segment.MaxIndexSizeBytes = 1 * 2 << 20
	cfg.Segment.MaxStoreSizeBytes = 1 * 2 << 20
	tempDir, err := ioutil.TempDir("", "wal-conc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := New(tempDir, &cfg)
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

func TestNextID(t *testing.T) {
	type test struct {
		input string
		want  string
	}

	tests := []test{
		{"0001", "0002"},
		{"0009", "0010"},
		{"0099", "0100"},
		{"0999", "1000"},
		{"1000", "1001"},
	}

	for _, tc := range tests {
		got := nextID(tc.input)
		if got != tc.want {
			t.Errorf("%s != %s", got, tc.want)
		}
	}
}

func TestReadWriteWithNewSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "append-segment")
	defer os.RemoveAll(dir)
	cfg := Config{}
	cfg.Segment.MaxIndexSizeBytes = 32
	cfg.Segment.MaxStoreSizeBytes = 1024

	wal, err := New(dir, &cfg)
	if err != nil {
		t.Fatal(err)
	}

	if wal.activeSegment.segmentID != "0001" {
		t.Error("first segment should be 0001")
	}

	records := []string{
		"first",
		"second",
		"third",
		"fourth",
		"fifth",
		"sixth",
	}
	var ids []uint64

	for i := 0; i < 2; i++ {
		id, err := wal.Append([]byte(records[i]))
		if err != nil {
			t.Error(err)
		}
		ids = append(ids, id)

		if wal.activeSegment.segmentID != "0001" {
			t.Error("first segment should be 0001")
		}
	}

	for i := 2; i < 4; i++ {
		id, err := wal.Append([]byte(records[i]))
		if err != nil {
			t.Error(err)
		}
		ids = append(ids, id)

		if wal.activeSegment.segmentID != "0002" {
			t.Error("second segment should be 0002")
		}
	}

	for i := 4; i < 6; i++ {
		id, err := wal.Append([]byte(records[i]))
		if err != nil {
			t.Error(err)
		}
		ids = append(ids, id)

		if wal.activeSegment.segmentID != "0003" {
			t.Error("third segment should be 0003")
		}
	}

	for i, id := range ids {
		data, err := wal.Read(id)
		if err != nil {
			t.Error(err)
		}
		if err != nil {
			t.Error(err)
		}
		if records[i] != string(data) {
			t.Error("read is not right")
		}
	}

	// remove 1 and 2 segment
	err = wal.Trim(5)
	if err != nil {
		t.Error(err)
	}

	if len(wal.segments) != 1 || wal.segments[0].segmentID != "0003" {
		t.Error("should remove two first segment")
	}
}

