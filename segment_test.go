package wal

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegmentReadWrite(t *testing.T) {
	idxFile, err := ioutil.TempFile("", "segemet-index-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(idxFile.Name())

	storeFile, err := ioutil.TempFile("", "segment-store-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(storeFile.Name())

	segment, err := newSegment(idxFile.Name(), storeFile.Name(), defaultConfig(Config{}))
	if err != nil {
		t.Fatal(err)
	}

	messages := []string{
		"hello",
		"test",
		"abc",
	}

	var ids []uint64

	for _, message := range messages {
		id, err := segment.write([]byte(message))
		if err != nil {
			t.Error(err)
		}

		ids = append(ids, id)
	}

	for i, id := range ids {
		data, err := segment.read(id)
		if err != nil {
			t.Error(err)
		}
		if string(data) != messages[i] {
			t.Error("data is not the same")
		}
	}
}

func TestRemoveSegment(t *testing.T) {
	i, _ := ioutil.TempFile("", "index-remove")
	s, _ := ioutil.TempFile("", "store-remove")
	_ = i.Close()
	_ = s.Close()

	seg, _ := newSegment(i.Name(), s.Name(), defaultConfig(Config{}))
	err := seg.remove()
	if err != nil {
		t.Error(err)
	}

	_, err = os.Stat(i.Name())
	if !errors.Is(err, os.ErrNotExist) {
		fmt.Println(err)
		t.Error("should delete index file")
	}
	_, err = os.Stat(s.Name())
	if !errors.Is(err, os.ErrNotExist) {
		t.Error("should delete store file")
	}
}
