package wal

import (
	"io/ioutil"
	"os"
	"testing"
)

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
	cfg := configDefautls(Config{})
	if cfg.Segment.MaxIndexSizeBytes != defaultIndexSize {
		t.Error("default value for index is not set")
	}

	if cfg.Segment.MaxStoreSizeBytes != defaultStoreSize {
		t.Error("default value for store is not set")
	}

	cfg = Config{}
	cfg.Segment.MaxIndexSizeBytes = 1000
	cfg.Segment.MaxStoreSizeBytes = 2000

	cfg = configDefautls(cfg)
	if cfg.Segment.MaxIndexSizeBytes != 1000 {
		t.Error("should not change non zero value")
	}
	if cfg.Segment.MaxStoreSizeBytes != 2000 {
		t.Error("should not change non zero value")
	}
}
