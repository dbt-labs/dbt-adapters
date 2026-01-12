## Fix

- Ensure BigQuery query result polling enforces the configured job execution timeout plus a short buffer to avoid indefinite hangs when connections drop mid-query. ([#1500](https://github.com/dbt-labs/dbt-adapters/issues/1500))
