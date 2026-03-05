# Root Cause Analysis: Snowflake Test OOM in CI

## What's happening

The GitHub Actions runner (ubuntu-22.04, **7 GB RAM**) receives a shutdown signal (OOM kill) ~8-10 minutes into the Snowflake test run. All tests that execute before the kill are **passing** — this is purely a memory exhaustion issue, not a test failure.

The failure started between Feb 24 17:04 UTC (last success) and Feb 24 23:09 UTC (first failure).

## Primary cause: `-n auto` parallel workers + heavy transitive dependencies

The pytest config in `dbt-snowflake/pyproject.toml:46` sets:
```
addopts = "-v --color=yes -n auto"
```

This spawns **one worker per CPU core** (4 workers: gw0-gw3 on the runner). Each worker process independently loads the **entire** dependency stack:

| Dependency | Runtime memory cost | Loaded per-worker? |
|---|---|---|
| **botocore** | ~50-100+ MB (lazy-loads massive AWS JSON service models) | Yes |
| **snowflake-connector-python** | ~30-50 MB (C extensions, OCSP, auth) | Yes |
| **cryptography** | ~15-20 MB (Rust/C OpenSSL bindings) | Yes |
| **dbt-core + dbt-adapters + dbt-common** | ~50-80 MB (manifest, macro loader, Jinja) | Yes |
| **ddtrace** (CI uses `--ddtrace`) | ~30-50 MB (bytecode instrumentation, profiling) | Yes |
| Python interpreter overhead | ~30 MB | Yes |

**Rough estimate: ~200-300 MB per worker x 4 workers = ~800 MB-1.2 GB just for imports, before any test data.**

On a 7 GB runner, this leaves limited headroom for actual test execution, OS overhead, and hatch/pip processes.

## What tipped it over the edge

There are no lockfiles in this repo — dependencies resolve to **latest compatible versions** on every CI run. Two key changes around Feb 24:

1. **botocore** (released daily, `1.42.56` on Feb 24) — This package grows monotonically as AWS adds service definitions. It's the heaviest transitive dep of snowflake-connector-python and is **completely unpinned**. Over months of daily releases, the cumulative size growth can be significant.

2. **botocore is a core dep of snowflake-connector-python** (needed for S3 staging), so it's always installed and imported even though dbt tests don't use S3.

## Contributing factors

1. **No dependency lockfile** — Fresh resolution on every CI run means any transitive dep bump can break things.

2. **The `[secure-local-storage]` extra** pulls in `keyring` which adds small but non-zero overhead per process.

3. **CI env uses ddtrace** (`hatch.toml:85` — `unit-tests = "python -m pytest tests/unit --ddtrace"`) which instruments all code at runtime, adding memory per worker.

4. **Unit tests create full adapter instances** per test — `test_snowflake_adapter.py` creates `SnowflakeAdapter` with `get_context("spawn")` and runs `ManifestLoader.load_macros()` for each of its 25 tests, compounding memory in each worker.

## Existing awareness

PR [#1681](https://github.com/dbt-labs/dbt-adapters/pull/1681) (`swap-to-mac-os-runner`) already proposes switching Snowflake tests to `macos-latest` which has **14 GB RAM** — confirming the team recognizes this as a runner memory issue.

## Recommended fixes (ordered by impact)

| Fix | Effort | Impact |
|---|---|---|
| **Reduce parallelism**: Change `-n auto` to `-n 2` or `-n 1` in pyproject.toml for CI | Low | High — halves/quarters per-process memory |
| **Switch to larger runner** (PR #1681's macOS approach, or `ubuntu-22.04-xl`) | Low | High — doubles available RAM |
| **Pin botocore** upper bound in pyproject.toml or add a constraints file | Low | Medium — prevents unbounded growth |
| **Drop `--ddtrace` from unit tests** (only needed for integration tracing) | Low | Medium — saves ~30-50 MB per worker |
| **Add a pip constraints/lockfile** for CI reproducibility | Medium | High — prevents surprise dep upgrades |
| **Refactor unit tests** to use shared fixtures instead of per-test adapter creation | High | Medium — reduces per-test memory churn |

## Appendix: Dependency details

### snowflake-connector-python version

- Constraint: `>=4.2.0,<5.0.0`
- Resolves to: **4.3.0** (released Feb 12, 2026)
- Only 2 versions exist in this range: 4.2.0 and 4.3.0
- Dependencies between 4.2.0 and 4.3.0 are **identical** (setup.cfg unchanged)

### Key transitive dependencies (unpinned)

| Package | Resolved version | Size | Notes |
|---|---|---|---|
| botocore | 1.42.58 | 14.3 MB | Daily releases, grows monotonically |
| cryptography | 46.0.5 | 6.8 MB | Released Feb 10 |
| snowflake-connector-python | 4.3.0 | 11.5 MB | C extensions |
| boto3 | 1.42.58 | 0.1 MB | Thin wrapper around botocore |
| keyring | 25.7.0 | 0.1 MB | From `secure-local-storage` extra |

### Known open memory issues in snowflake-connector-python

| Issue | Description |
|---|---|
| [#1018](https://github.com/snowflakedb/snowflake-connector-python/issues/1018) | Circular references in `SnowflakeConnection` prevent GC after `close()` |
| [#1531](https://github.com/snowflakedb/snowflake-connector-python/issues/1531) | `fetchall()` causes memory issues with large datasets |
| [#1875](https://github.com/snowflakedb/snowflake-connector-python/issues/1875) | Memory leak after interrupting a SELECT query mid-fetch |

### CI run history

| Date (UTC) | Run ID | Result | Duration |
|---|---|---|---|
| Feb 20, 15:29 | 22230039822 | SUCCESS | 30 min |
| Feb 24, 17:04 | 22361420439 | SUCCESS | 38 min |
| Feb 24, 23:09 | 22374080305 | FAILURE | ~8 min (killed) |
| Feb 26, 11:20 | 22439849405 | FAILURE | ~9.5 min (killed) |
| Feb 26, 12:29 | 22442112416 | FAILURE | ~8.5 min (killed) |
| Feb 27, 00:40 | 22467592478 | FAILURE | ~8 min (killed) |
