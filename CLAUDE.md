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

## Development Principles

**# 永远性能优先** (Always prioritize performance)

This is a high-performance storage engine. Performance should be the primary consideration:
- Prefer `sync.Pool` for object reuse to reduce GC pressure
- Use efficient serialization (Protobuf over JSON)
- Minimize allocations in hot paths
- Consider cache locality and memory layout
- Benchmark before and after optimizations

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


---

## 1. 技术栈与环境 (Tech Stack & Environment)

- **语言**: Go (>= 1.24)
- **构建/测试/质量**:
  - **构建**: 标准 `go build` 或 `Makefile`
  - **测试**: 标准 `go test`
  - **代码规范**: `gofmt`, `goimports`
  - **静态检查**: `golangci-lint` (配置文件为 `.golangci.yml`)

---

## 2. 架构与代码规范 (Architecture & Code Style)

- **项目结构**: 严格遵循标准的Go项目布局 (https://go.dev/doc/modules/layout)。核心业务逻辑必须放在`internal/`目录下。
- **错误处理**: **[强制]** 所有错误返回必须使用 `fmt.Errorf("...: %w", err)` 的方式进行错误包装(wrapping)，以保留上下文和调用栈。绝不允许直接 `return err`。
- **日志**: **[强制]** 必须使用标准库 `log/slog` 进行结构化日志记录。日志信息中必须包含关键的上下文信息（如`userID`, `traceID`）。
- **接口设计**: 遵循Go语言的接口设计哲学——“接口应该由消费者定义”。优先定义小的、单一职责的接口。

---

## 3. Git与版本控制 (Git & Version Control)

- **Commit Message规范**: **[严格遵循]** Conventional Commits 规范 (https://www.conventionalcommits.org/)。
  - 格式: `<type>(<scope>): <subject>`
  - 当被要求生成commit message时，必须遵循此格式。

---

## 4. AI协作指令 (AI Collaboration Directives)

- **[原则] 优先标准库**: 在有合理的标准库解决方案时，优先使用标准库，而不是引入新的第三方依赖。
- **[流程] 审查优先**: 当被要求实现一个新功能时，你的第一步应该是先用`@`指令阅读相关代码，理解现有逻辑，然后以列表形式提出你的实现计划，待我确认后再开始编码。
- **[实践] 表格驱动测试**: 当被要求编写测试时，你必须优先编写**表格驱动测试（Table-Driven Tests）**，这是本项目推崇的测试风格。
- **[实践] 并发安全**: 当你的代码中涉及到并发（goroutines, channels）时，**必须**明确指出潜在的竞态条件风险，并解释你所使用的并发安全措施（如mutex, channel）。
- **[产出] 解释代码**: 在生成任何复杂的代码片段后，请用注释或在对话中，简要解释其核心逻辑和设计思想。

---
