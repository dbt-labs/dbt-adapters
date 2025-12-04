# Contributing to `dbt-athena`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## 1. Configure Environment

From the `dbt-athena` directory, run `hatch run setup` to create a `test.env` file. Populate this file with your Athena connection details.

* `DBT_TEST_ATHENA_S3_STAGING_DIR`
* `DBT_TEST_ATHENA_S3_TMP_TABLE_DIR`
* `DBT_TEST_ATHENA_REGION_NAME`
* `DBT_TEST_ATHENA_DATABASE`
* `DBT_TEST_ATHENA_SCHEMA`
* `DBT_TEST_ATHENA_WORK_GROUP`
* `DBT_TEST_ATHENA_THREADS`
* `DBT_TEST_ATHENA_POLL_INTERVAL`
* `DBT_TEST_ATHENA_NUM_RETRIES`
* `DBT_TEST_ATHENA_AWS_PROFILE_NAME`

## 2. Run Tests

Once `test.env` is configured, you can run tests using `hatch`:

```shell
# run all integration tests
$ hatch run integration-tests

# run tests for a specific module
$ hatch run integration-tests tests/functional/adapter/test_basic_hive.py

# run a specific test class
$ hatch run integration-tests tests/functional/adapter/test_basic_hive.py::TestSimpleMaterializationsHive
```
