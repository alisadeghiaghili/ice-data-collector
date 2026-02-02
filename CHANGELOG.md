# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-02-02

### 🚀 Major Improvements

#### Added
- **Comprehensive Logging System**
  - File and console handlers with different log levels
  - Structured logs with function names and line numbers
  - Daily log file rotation
  - Debug mode for troubleshooting

- **Robust Error Handling**
  - Retry logic with exponential backoff using `tenacity`
  - Graceful degradation on partial failures
  - Detailed error messages with stack traces
  - Per-currency error isolation (one failure doesn't stop entire pipeline)

- **Type Hints**
  - Complete type annotations for better IDE support
  - Type safety for function parameters and return values
  - Better code documentation through types

- **Configuration Management**
  - Centralized `Config` class for all settings
  - Environment variable support for connection strings
  - Easy customization without code changes

- **Performance Monitoring**
  - Progress tracking with step indicators
  - Duration metrics for each stage
  - Record count summaries

- **Better Architecture**
  - Object-oriented design with `CurrencyETL` class
  - Separation of concerns
  - Improved testability
  - Backward compatibility maintained

#### Enhanced
- **API Fetching**
  - Automatic retry on network failures (3 attempts)
  - Configurable timeout (30 seconds)
  - Session management for connection pooling
  - Better error messages for API failures

- **Data Validation**
  - Null date detection and logging
  - Persian digit conversion with error handling
  - Invalid data skipping instead of crashing

- **Database Operations**
  - Better connection string handling
  - Fast executemany for performance
  - Detailed success/failure reporting
  - Transaction safety

#### Fixed
- JDate formatting errors now logged instead of crashing
- Persian number cleaning handles edge cases
- Empty API responses handled gracefully
- Database connection errors properly caught and logged

### Breaking Changes
- Minimum Python version: 3.7+
- New dependency: `tenacity>=8.0.0`
- Main function now returns exit code

### Migration Guide

**From v1.x to v2.0:**

```python
# Old way (still works)
from AutoTrowel_Documented import incremental_load
incremental_load(CONNECTION_STRING)

# New way (recommended)
from AutoTrowel_Documented import CurrencyETL
etl = CurrencyETL(CONNECTION_STRING)
etl.run()
```

## [1.0.0] - 2026-01-25

### Added
- Initial release
- Basic ETL pipeline for ICE.ir currency data
- Incremental loading functionality
- Persian date conversion
- SQL Server integration
- Support for 8 currencies (USD, EUR, AED, JPY, RUB, CNY, IQD, INR)
- Bill and WireTransfer transaction types

---

**Legend:**
- 🚀 Major features
- ✨ Minor features
- 🐛 Bug fixes
- 📄 Documentation
- ⚡ Performance
- 🔒 Security
