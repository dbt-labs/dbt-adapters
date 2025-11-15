# Contributing to `dbt-bigquery`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## 1. Configure Environment

From the `dbt-bigquery` directory, run `hatch run setup` to create a `test.env` file. Populate this file with your BigQuery connection details.

* `BIGQUERY_TEST_NO_ACCESS_DATABASE`
* `BIGQUERY_TEST_SERVICE_ACCOUNT_JSON`
* `BIGQUERY_TEST_ICEBERG_BUCKET`
* `DBT_TEST_USER_1`
* `DBT_TEST_USER_2`
* `DBT_TEST_USER_3`
* `COMPUTE_REGION`
* `DATAPROC_CLUSTER_NAME`
* `GCS_BUCKET`

## 2. Run Tests

Once `test.env` is configured, you can run tests using `hatch`:

```shell
# run all integration tests
$ hatch run integration-tests

# run tests for a specific module
$ hatch run integration-tests tests/functional/adapter/test_basic.py

# run a specific test class
$ hatch run integration-tests tests/functional/adapter/test_basic.py::TestSimpleMaterializationsBigQuery
```
