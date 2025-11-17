# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Replace `event!` macro with specific logging macros (`info!`, `error!`, `warn!`) across library code for better code clarity and idiomatic Rust logging
  - Updated: all plant files, `src/api/receiver_api.rs`, `src/request_handler.rs`

## [0.5.0] - 2025-11-16

> **📖 Migration Guide:** See [MIGRATION_0.5.0.md](MIGRATION_0.5.0.md) for detailed step-by-step migration instructions.

### Breaking Changes

#### Connection API Changes
- **Plant constructors renamed**: `new()` → `connect()` across all plants
- **Return type changed**: `connect()` now returns `Result<Plant, Box<dyn std::error::Error>>`
- **Required parameter**: All plants now require a `ConnectStrategy` parameter
- Enables proper error handling instead of panics and explicit connection strategy selection

#### Configuration API Changes
- **New unified configuration**: `RithmicConfig` replaces separate account/connection info types
  - Old types (`AccountInfo`, `RithmicConnectionInfo`, `RithmicConnectionSystem`) are deprecated
  - Migration path provided via `From`/`TryFrom` trait implementations
- **Environment handling**: `RithmicEnv` replaces `RithmicConnectionSystem`
  - More idiomatic enum naming
  - Better integration with configuration builder

#### Error Handling Changes
- **Heartbeat error visibility**: Heartbeat responses now delivered through subscription channel
  - `ResponseHeartbeat` changed from `is_update: false` → `is_update: true`
  - Consumers must check `error` field on heartbeat responses to detect connection issues
  - Breaking for applications that assumed heartbeats wouldn't appear in subscriptions
- **Forced logout events**: Now delivered through subscription channel for visibility
  - `ForcedLogout` changed from `is_update: false` → `is_update: true`
  - Applications must handle forced logout events to implement reconnection logic
- **No more panics**: Error responses from server no longer panic, sent to subscription channel instead

### Added

#### Connection Strategies
- New `ConnectStrategy` enum with three modes:
  - **`Simple`**: Single connection attempt (recommended default, fast-fail)
  - **`Retry`**: Indefinite retries with exponential backoff on same URL
  - **`AlternateWithRetry`**: Alternates between primary and beta URLs with retries
- Retry strategies now retry indefinitely instead of limiting to 15 attempts
- Maximum backoff capped at 60 seconds to ensure at most one login attempt per minute
- Prevents excessive load on Rithmic servers during extended outages

#### Unified Configuration API
- `RithmicConfig`: Modern, ergonomic configuration type combining account and connection fields
- `RithmicEnv`: Environment selection enum (Demo, Live, Test)
- `ConfigError`: Type-safe error handling for configuration operations
- `from_env()`: Load configuration from environment variables with proper error handling
- `from_dotenv()`: Load configuration from .env file (requires `dotenv` feature)
- `RithmicConfigBuilder`: Builder pattern for programmatic configuration
- Comprehensive unit tests (15 tests) covering all configuration scenarios

#### Connection Health Monitoring
- Heartbeat responses now include error information in subscription channel
- Forced logout events delivered through subscription channel
- Applications can monitor connection health in real-time
- Examples added showing proper heartbeat timeout tracking

#### Documentation
- Comprehensive documentation for connection strategies
- Connection timeout and retry behavior documented
- Migration guide for deprecated types in `connection_info` module
- Real-world examples showing proper error handling and connection monitoring
- Examples updated to demonstrate new unified configuration API

### Fixed

#### Critical Panic Fixes
- Fixed panic on unknown message types by adding proper error handling (#3)
  - Unknown message types now logged and gracefully handled
  - Added `UnknownMessage` variant to handle unexpected protocol messages
- Fixed panic on error responses in ticker plant (#2)
  - Error responses from `buf_to_message()` now handled gracefully
  - Errors sent through subscription channel for consumer handling
- Fixed panic on heartbeat errors across all plants
  - Broadcast send errors now handled gracefully instead of unwrapping
  - No more crashes on channel receiver drops

#### Consistency Fixes
- Fixed inconsistent heartbeat logic across plants (#9)
  - All plants (ticker, order, pnl, history) now only send heartbeats after login
  - Prevents protocol violations from pre-login heartbeats
  - Unified behavior across all plant implementations
- Fixed MessageType decode unwrap with proper error handling (#4)
  - Removed `.unwrap()` calls in message decoding
  - Proper error propagation through Result types

#### Code Quality
- Removed `#[allow(dead_code)]` annotations from valid public API methods (#11)
  - `request_new_order`, `request_exit_position`, `request_show_brackets`, `request_show_bracket_stops`
  - Added comprehensive documentation for these public API methods
  - Improved library API clarity

### Deprecated

The following types are deprecated and will be removed in a future version:
- `AccountInfo` - Use `RithmicConfig` instead
- `RithmicConnectionInfo` - Use `RithmicConfig` instead
- `RithmicConnectionSystem` - Use `RithmicEnv` instead
- `get_config()` function - Use `RithmicConfig::from_env()` or builder pattern

Migration helpers provided via trait implementations maintain backward compatibility.

### Changed

#### API Consistency
- Unified error handling pattern across all plants
  - Consistent routing based on `is_update` flag
  - Simplified message handling logic
  - No panics in production code

#### Internal Improvements
- Updated `RithmicSenderApi` to use `RithmicConfig` and `RithmicEnv`
- Simplified routing logic using `is_update` flag instead of message type checks
- Improved type safety by replacing panics with proper error types

### Migration Guide

For detailed migration instructions with code examples, see **[MIGRATION_0.5.0.md](MIGRATION_0.5.0.md)**.

Quick summary:
- Replace `Plant::new()` → `Plant::connect(&config, strategy)`
- Replace `AccountInfo` → `RithmicConfig::from_env()` or builder pattern
- Choose `ConnectStrategy` (Simple/Retry/AlternateWithRetry)
- Add error handling for heartbeats and forced logouts in subscription channel

### Future Plans

- Remove `dotenv` dependency - move to optional feature (planned for 0.6.0)
  - Configuration will work without .env files by default
  - `from_dotenv()` will require opt-in feature flag
  - Reduces mandatory dependencies for library users

## [0.4.2] - 2025-11-15

Previous stable release. See git history for earlier changes.

---

## Version History Summary

- **0.5.0** (2025-11-16): Major stability and API improvements - Connection strategies, unified config, panic fixes, connection health monitoring
- **0.4.2** (2025-11-15): Previous stable release

[Unreleased]: https://github.com/pbeets/rithmic-rs/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/pbeets/rithmic-rs/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/pbeets/rithmic-rs/releases/tag/v0.4.2
