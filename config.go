package wal

const (
	defaultStoreSize = 1 << 10
	defaultIndexSize = 1 << 10
)

// Config stores embedded log configuration data
// MaxIndexSizeBytes should be multiple of 16
type Config struct {
	Segment struct {
		MaxStoreSizeBytes uint64
		MaxIndexSizeBytes uint64
	}
}

var defaultConfig = Config{Segment: struct {
	MaxStoreSizeBytes uint64
	MaxIndexSizeBytes uint64
}{MaxStoreSizeBytes: defaultStoreSize, MaxIndexSizeBytes: defaultIndexSize}}
