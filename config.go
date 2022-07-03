package wal

// Config stores embedded log configuration data
// MaxIndexSizeBytes should be multiple of 16
type Config struct {
	Segment struct {
		MaxStoreSizeBytes uint64
		MaxIndexSizeBytes uint64
	}
}
