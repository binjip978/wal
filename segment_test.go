package wal

import (
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

	segment, err := newSegment(idxFile.Name(), storeFile.Name(), configDefautls(Config{}))
	if err != nil {
		t.Fatal(err)
	}

	messages := []string{
		"hello",
		"test",
		"abc",
	}

	var ids []recordID

	for _, message := range messages {
		id, err := segment.write([]byte(message))
		if err != nil {
			t.Error(err)
		}

		ids = append(ids, id)
	}

	for i, id := range ids {
		r, err := segment.read(id)
		if err != nil {
			t.Error(err)
		}
		if r.id != id {
			t.Error("id is not the same")
		}
		if string(r.data) != messages[i] {
			t.Error("data is not the same")
		}
	}
}
