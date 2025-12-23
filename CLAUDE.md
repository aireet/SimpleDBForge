# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Generate protobuf code (required after modifying .proto files)
make proto

# Run all tests
go test ./...

# Run tests for specific package
go test ./lsm/core -v
go test ./lsm/pkg -v
go test ./lsm/utils -v

# Run benchmarks
go test -bench=. -benchmem ./...

# Run with coverage
go test -cover ./...

# Race detection
go test -race ./...
```

## Architecture Overview

SimpleDBForge is an educational LSM (Log-Structured Merge-tree) storage engine implementation in Go. Currently, only the in-memory components are implemented:

### Core Components

1. **MemTable** (`lsm/core/memtable.go`) - In-memory write buffer using a skip list, with write-ahead logging for durability
2. **WAL** (`lsm/core/wal.go`) - Write-Ahead Log for crash recovery, using little-endian binary format with length-prefixed protobuf entries
3. **SkipList** (`lsm/pkg/skip_list.go`) - Probabilistic data structure for O(log n) lookups

### Data Flow

- **Write**: Entry serialized to protobuf -> written to WAL -> fsync'd -> inserted into SkipList
- **Read**: Direct lookup in SkipList
- **Recovery**: WAL read in batches via channel -> entries replayed into SkipList

### Key Design Patterns

- **Write-Ahead Logging**: All writes logged before being applied to MemTable
- **Tombstone deletion**: Entries have a `tombstone` field for soft deletes (LSM pattern)
- **Timestamp versioning**: Keys may include `@timestamp` suffix (e.g., `user:123@1640995200`), sorted in reverse chronological order
- **Buffer pooling**: `sync.Pool` used for `bytes.Buffer` reuse to reduce GC pressure

### Entry Schema (Protocol Buffers)

```
message Entry {
    string key       = 1;
    bytes value      = 2;
    bool tombstone   = 3;  // Deletion marker
    int64 version    = 4;  // MVCC version
}
```

### WAL Storage Format

```
[8 bytes: data length (little-endian)][N bytes: protobuf Entry]...
```

### Not Yet Implemented

- SSTable (Sorted String Table) - on-disk sorted files
- MemTable flushing to SSTable
- SSTable compaction
- Manifest (metadata about SSTables)
- Block cache
- Bloom filter
- Multi-level SSTable hierarchy

## Notes

- Go version: 1.24.6
- Code comments are in Chinese
- Function name typo: `NewMebTable` should be `NewMemTable` (exists in memtable.go)
