# Contributing to `dbt-snowflake`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## 1. Configure Environment

From the `dbt-snowflake` directory, run `hatch run setup` to create a `test.env` file. Populate this file with your Snowflake connection details.

* `SNOWFLAKE_TEST_ACCOUNT`
* `SNOWFLAKE_TEST_ALT_DATABASE`
* `SNOWFLAKE_TEST_ALT_WAREHOUSE`
* `SNOWFLAKE_TEST_DATABASE`
* `SNOWFLAKE_TEST_OAUTH_CLIENT_ID`
* `SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET`
* `SNOWFLAKE_TEST_OAUTH_REFRESH_TOKEN`
* `SNOWFLAKE_TEST_PASSWORD`
* `SNOWFLAKE_TEST_QUOTED_DATABASE`
* `SNOWFLAKE_TEST_USER`
* `SNOWFLAKE_TEST_WAREHOUSE`
* `SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE`
* `DBT_TEST_USER_1`
* `DBT_TEST_USER_2`
* `DBT_TEST_USER_3`

## 2. Run Tests

Once `test.env` is configured, you can run tests using `hatch`:

```shell
# run all integration tests
$ hatch run integration-tests

# run tests for a specific module
$ hatch run integration-tests tests/functional/adapter/test_basic.py

# run a specific test class
$ hatch run integration-tests tests/functional/adapter/test_basic.py::TestSimpleMaterializationsSnowflake
```
