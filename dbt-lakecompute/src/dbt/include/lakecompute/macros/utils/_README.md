# Ported cross-database utility macros

These six macros are ports of the `duckdb__*` implementations in
[dbt-duckdb](https://github.com/duckdb/dbt-duckdb) 1.11.0, which is licensed under the
Apache License 2.0. Each file carries the attribution in a header comment.

They are vendored rather than inherited: `AdapterPlugin` declares no `dependencies=`, because
dbt-duckdb redefines the bare `create_table_as` and `run_hooks` macros and its `apply_grants`
silently discards every grant.
