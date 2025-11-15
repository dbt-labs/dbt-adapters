# Contributing to `dbt-redshift`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## 1. Configure Environment

From the `dbt-redshift` directory, run `hatch run setup` to create a `test.env` file. Populate this file with your Redshift connection details.

* `REDSHIFT_TEST_HOST`
* `REDSHIFT_TEST_PORT`
* `REDSHIFT_TEST_DBNAME`
* `REDSHIFT_TEST_USER`
* `REDSHIFT_TEST_PASS`
* `REDSHIFT_TEST_REGION`
* `REDSHIFT_TEST_CLUSTER_ID`
* `REDSHIFT_TEST_IAM_USER_PROFILE`
* `REDSHIFT_TEST_IAM_USER_ACCESS_KEY_ID`
* `REDSHIFT_TEST_IAM_USER_SECRET_ACCESS_KEY`
* `REDSHIFT_TEST_IAM_ROLE_PROFILE`
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
$ hatch run integration-tests tests/functional/adapter/test_basic.py::TestSimpleMaterializationsRedshift
```
