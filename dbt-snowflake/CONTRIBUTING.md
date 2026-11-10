# Contributing to `dbt-snowflake`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## Integration tests

Integration tests require access to a Snowflake account with necessary permissions to create databases, schemas, tables, and roles.

### Configure environment variables

Running `hatch run setup` will create a `test.env` file from `test.env.example` in this package directory if one does not already exist. Update `test.env` with your Snowflake connection details:

```bash
SNOWFLAKE_TEST_ACCOUNT=<account_identifier>
SNOWFLAKE_TEST_USER=<user>
SNOWFLAKE_TEST_PASSWORD=<password>
SNOWFLAKE_TEST_WAREHOUSE=<warehouse>
SNOWFLAKE_TEST_DATABASE=<primary_test_database>
SNOWFLAKE_TEST_ALT_DATABASE=<secondary_test_database>
SNOWFLAKE_TEST_ALT_WAREHOUSE=<secondary_warehouse>
```

> [!WARNING]
> Never commit `test.env` or any file containing live credentials to git.

### Run integration tests

Run integration tests from within the `dbt-snowflake` directory:

```shell
# Run all integration tests
$ hatch run integration-tests

# Run tests in a specific module
$ hatch run integration-tests -- tests/functional/path/to/test_file.py

# Run a specific test class or method
$ hatch run integration-tests -- tests/functional/path/to/test_file.py::TestClass::test_method
```
