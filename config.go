package wal

type Config struct {
	Segment struct {
		MaxStoreSizeBytes uint64
		MaxIndexSizeBytes uint64
	}
}
