## Embedded Write Ahead Log

Simple multi segment write ahead log. Writes two type of files files *.index and *.store.
Configurable  maximum index and store file sizes.

Files stucuture:

- Index record structure:
[__recordID__ (8 bytes)][__recordOffset__ (8 bytes)]
- Store record structure:
[__size__ (8 bytes)][__data__ (variable bytes)]

### Usage example

```go
wl, _ := wal.New(".", nil)
offset, _ := wl.Append(data)
data, _ := wl.Read(offset)
wl.Close()
```
