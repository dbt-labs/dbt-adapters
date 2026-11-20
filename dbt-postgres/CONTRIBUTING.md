# Contributing to `dbt-postgres`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## Integration tests

Integration tests require a running PostgreSQL instance.

### Configure environment variables

Running `hatch run setup` will create a `test.env` file from `test.env.example` in this package directory if one does not already exist. Update `test.env` with your instance's connection credentials:

```bash
POSTGRES_TEST_HOST=<host>
POSTGRES_TEST_PORT=5432
POSTGRES_TEST_USER=<user>
POSTGRES_TEST_PASS=<password>
POSTGRES_TEST_DATABASE=<database>
POSTGRES_TEST_THREADS=4

DBT_TEST_USER_1=dbt_test_user_1
DBT_TEST_USER_2=dbt_test_user_2
DBT_TEST_USER_3=dbt_test_user_3
```

> [!WARNING]
> Never commit `test.env` or any file containing live credentials to git.

### Run integration tests

Run integration tests from within the `dbt-postgres` directory:

```shell
# Run all integration tests
$ hatch run integration-tests

# Run all tests in a specific module
$ hatch run integration-tests -- tests/functional/path/to/test_file.py

# Run a specific test class or method
$ hatch run integration-tests -- tests/functional/path/to/test_file.py::TestClass::test_method
```
