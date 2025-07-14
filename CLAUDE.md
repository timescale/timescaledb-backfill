# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

TimescaleDB Backfill is a Rust-based command-line tool for migrating TimescaleDB hypertable data using the dual-write and backfill pattern. It efficiently copies data between TimescaleDB instances without requiring intermediate storage or decompressing compressed chunks.

## Development Commands

### Build and Test
- `cargo build` - Build the project in debug mode
- `cargo build --release` - Build optimized release version  
- `cargo test` - Run all tests (unit and integration)
- `cargo test --bins && cargo test -p test-common` - Run only unit tests (faster)
- `make test` - Run tests with custom thread count (RUST_TEST_THREADS=4)
- `make short-test` - Run only unit tests

### Linting and Formatting
- `cargo fmt` - Format code
- `cargo clippy --all-targets --all-features -- -D warnings` - Lint with clippy
- `make lint` - Run both fmt and clippy
- `make fmt` - Format code
- `make clippy` - Run clippy

### Running the Tool
- `cargo run --release --` - Run the tool (add arguments after --)
- `make run` - Run via Makefile

### Integration Testing
- `make test-image` - Run integration tests against Docker image
- Uses environment variables: `BF_TEST_PG_VERSION`, `BF_TEST_TS_VERSION`

## Architecture

### Core Components

**Main CLI (`src/main.rs`)**: Entry point with three main subcommands:
- `stage` - Creates copy tasks for hypertable chunks
- `copy` - Processes tasks and copies chunks  
- `clean` - Removes administrative schema

**Key Modules**:
- `connect.rs` - Database connection management for Source/Target
- `execute.rs` - Core data copying logic with transaction handling
- `task.rs` - Task management and chunk processing
- `timescale.rs` - TimescaleDB-specific operations and version handling
- `workers.rs` - Concurrent processing with worker pools
- `verify.rs` - Data verification between source and target
- `caggs.rs` - Continuous aggregates handling
- `storage.rs` - Administrative schema management
- `telemetry.rs` - Usage telemetry collection

### Database Schema

The tool creates a `__backfill` administrative schema in the target database to track:
- Copy tasks and their status
- Chunk metadata and processing state
- Verification results
- Progress tracking

### Key Features

**Chunk-based Processing**: Operates directly on TimescaleDB chunks for efficiency
**Transactional Safety**: Uses transactions to ensure data consistency
**Parallel Processing**: Worker pool for concurrent chunk processing
**Verification**: Optional data verification between source and target
**Telemetry**: Collects usage statistics (can be disabled)

### Dependencies

- PostgreSQL client libraries via `tokio-postgres`
- Async runtime with `tokio`
- CLI parsing with `clap`
- Logging with `tracing`
- Private dependencies: `telemetry-client`, `test-common`

## Testing

### Test Structure
- `tests/integration/` - Integration tests requiring database setup
- Unit tests embedded in source files
- `test-common/` - Shared testing utilities
- `test-manual/` - Manual testing scripts and SQL

### Test Configuration
- Supports PostgreSQL versions 15, 16, 17
- Supports TimescaleDB versions 2.16-2.21
- Uses testcontainers for isolated database testing
- Environment variables control test database versions

## Build Configuration

### Cargo.toml Features
- Release builds use LTO and single codegen unit for optimization
- Debug symbols included in release builds
- Specific Rust toolchain: 1.72.0

### CI/CD
- GitHub Actions workflow in `.github/workflows/ci.yaml`
- Runs on multiple PostgreSQL and TimescaleDB version combinations
- Includes linting, formatting, and comprehensive testing
- Uses private GitHub token for private dependency access