package wal

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestIndexSize(t *testing.T) {
	f, err := ioutil.TempFile("", "index-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	cfg := Config{}
	cfg.Segment.MaxIndexSizeBytes = 16

	i, err := newIndex(f.Name(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	_, err = i.write(0)
	if err != nil {
		t.Error(err)
	}

	if i.size != 16 {
		t.Error("index size should be 16")
	}

	_, err = i.write(16)
	if err != ErrNoIndexSpaceLeft {
		t.Error("wrong error returned")
	}
}

func TestIndexReadWrite(t *testing.T) {
	f, err := ioutil.TempFile("", "index-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	cfg := configDefautls(Config{})

	i1, err := newIndex(f.Name(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	ids := []uint64{}
	offsets := []uint64{0, 10, 21}

	for _, offset := range offsets {
		id, err := i1.write(offset)
		if err != nil {
			t.Error(err)
		}

		ids = append(ids, id)
	}

	for i, id := range ids {
		offset, err := i1.read(id)
		if err != nil {
			t.Error(err)
		}
		if offset != offsets[i] {
			t.Error("offsets are not the same")
		}
	}

	err = i1.close()
	if err != nil {
		t.Error(err)
	}

	i2, err := newIndex(f.Name(), cfg)
	if err != nil {
		t.Error(err)
	}

	for i, id := range ids {
		offset, err := i2.read(id)
		if err != nil {
			t.Error(err)
		}
		if offset != offsets[i] {
			t.Error("offsets are not the same")
		}
	}

}
