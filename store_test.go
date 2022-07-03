package wal

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestStoreReadWrite(t *testing.T) {
	messages := []string{
		"hello",
		"hello11",
		"hello-world",
	}

	f, err := ioutil.TempFile("", "store-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f.Name(), defaultConfig(Config{}))
	if err != nil {
		t.Fatal(err)
	}

	var offsets []uint64

	for _, message := range messages {
		offset, err := s.write([]byte(message))
		if err != nil {
			t.Error(err)
		}

		offsets = append(offsets, offset)
	}

	for i, offset := range offsets {
		b, err := s.read(offset)
		if err != nil {
			t.Error(err)
		}

		if messages[i] != string(b) {
			t.Errorf("data is wrong: %s != %s", messages[i], string(b))
		}
	}
}

func TestStoreSize(t *testing.T) {
	f, err := ioutil.TempFile("", "store-test-size")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	cfg := Config{}
	cfg.Segment.MaxStoreSizeBytes = 16

	s, err := newStore(f.Name(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.write([]byte("01234567"))
	if err != nil {
		t.Fatal(err)
	}

	if s.size != 16 {
		t.Error("size is wrong")
	}

	_, err = s.write([]byte{'a'})
	if err != ErrNoStoreSpaceLeft {
		t.Error("should return no space left error")
	}
}
