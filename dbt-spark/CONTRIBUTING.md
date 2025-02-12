# Contributing to `dbt-spark`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

`dbt-spark` uses [dagger](https://dagger.io/) to orchestrate its test suite.
This launches a virtual container or containers to test against.
`dagger` will be installed as part of running `hatch run setup`.

> [!NOTE]
> `dbt-spark` has not yet been configured to install `dbt-adpaters` and `dbt-tests-adapter` as local, editable packages.

## Test profiles

`dbt-spark` can test against multiple implementations, or "profile"s:

```
`--profile`: required, this is the kind of spark connection to test against

_options_:
  - "apache_spark"
  - "spark_session"
  - "spark_http_odbc"
  - "databricks_sql_endpoint"
  - "databricks_cluster"
  - "databricks_http_cluster"
```

To test against Apache Spark:

```shell
# run all tests for the apache_spark profile
$ hatch run integration-tests --profile "apache_spark"

# run tests for the apache_spark profile for a specific module
$ hatch --profile "apache_spark" --test-path tests/functional/adapter/test_basic.py

# run a specific test for the apache_spark profile
$ hatch --profile "apache_spark" --test-path tests/functional/adapter/test_basic.py::TestSimpleMaterializationsSpark::test_base
```
