# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is SimpleDB, a C++ database engine implementation with custom storage management, buffer pooling, and B-tree indexing. The project demonstrates core database concepts including page-based storage, buffer pool management, B-tree indices, SQL parsing, and query execution.

## Build System

The project uses CMake with C++17 standard:

```bash
# Build the project
mkdir -p build && cd build
cmake ..
make

# Run the database engine
./simpledb
```

## Architecture

### Core Components

- **Storage Layer**: `StorageManager` handles disk I/O operations, managing 4KB pages
- **Buffer Management**: `BufferPoolManager` implements LRU-based buffer pool with pin/unpin semantics
- **Indexing**: `BTree` provides B-tree indexing with configurable order (default: 4)
- **Query Processing**: `SQLParser` parses SQL statements and `Database` executes queries
- **Data Model**: `Record` handles variant-typed values (int, double, string) with serialization

### Key Design Patterns

- Page-based storage with fixed 4KB page size (`PAGE_SIZE = 4096`)
- Buffer pool uses frame-based management with LRU eviction
- B-tree nodes support both internal and leaf configurations
- SQL parser tokenizes and constructs query execution plans
- Database class orchestrates all components and maintains table metadata

### File Structure

```
src/
├── page.{h,cpp}                 - Page abstraction and memory management
├── storage_manager.{h,cpp}      - Disk I/O and page allocation
├── buffer_pool_manager.{h,cpp}  - Buffer pool with LRU replacement
├── btree.{h,cpp}               - B-tree implementation for indexing
├── record.{h,cpp}              - Record structure with variant values
├── sql_parser.{h,cpp}          - SQL tokenization and parsing
├── database.{h,cpp}            - High-level database interface
└── main.cpp                    - Demo application
```

## Development Commands

### Build and Run
```bash
# Build from root directory
mkdir -p build && cd build
cmake ..
make

# Run demo application
./simpledb
```

### Testing
```bash
# Build tests
cd build
make tests

# Run tests
./tests/tests
```

## Query Support

The database supports basic SQL operations:
- `CREATE TABLE table_name (column type, ...)`
- `INSERT INTO table_name VALUES (value1, value2, ...)`  
- `SELECT * FROM table_name [WHERE condition]`

Supported data types: `INT`, `VARCHAR`, with basic comparison operators (`=`, `<`, `>`, etc.).