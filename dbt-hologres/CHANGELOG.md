# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-12-30

### Features

- Initial release of dbt-hologres adapter
- Support for Alibaba Cloud Hologres data warehouse
- Uses Psycopg 3 for database connectivity
- PostgreSQL-compatible SQL syntax
- Full support for standard dbt materializations: table, view, incremental
- Support for Hologres Dynamic Tables (物化视图) with auto-refresh
- Multiple incremental strategies: append, delete+insert, merge, microbatch
- Full constraint support: primary keys, foreign keys, unique, not null, check
- SSL disabled by default for Hologres connections
- Case-sensitive username and password authentication
- Custom application_name with version tracking
- Comprehensive test suite
- Example project with sample models
- Complete documentation

### Breaking Changes

- None (initial release)

### Under the Hood

- Built on dbt-adapters framework v1.19.0+
- Requires Python 3.10+
- Requires dbt-core 1.8.0+
- Uses Psycopg 3 instead of Psycopg 2
