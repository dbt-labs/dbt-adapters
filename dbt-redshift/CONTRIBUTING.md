# Contributing to `dbt-redshift`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## Integration tests

Integration tests require access to an Amazon Redshift cluster or serverless workgroup.

### Configure environment variables

Running `hatch run setup` will create a `test.env` file from `test.env.example` in this package directory if one does not already exist. Update `test.env` with your instance credentials:

```bash
REDSHIFT_TEST_HOST=<cluster-or-workgroup-endpoint>
REDSHIFT_TEST_PORT=5439
REDSHIFT_TEST_DBNAME=<database>
REDSHIFT_TEST_USER=<user>
REDSHIFT_TEST_PASS=<password>
REDSHIFT_TEST_REGION=<aws-region>

# Optional: Cross-database and hyphenated database tests
REDSHIFT_TEST_CROSS_DBNAME=
REDSHIFT_TEST_DBNAME_W_HYPHEN=
```

> [!WARNING]
> Never commit `test.env` or any file containing live credentials to git.

### Run integration tests

Run integration tests from within the `dbt-redshift` directory:

```shell
# Run all non-flaky integration tests
$ hatch run integration-tests

# Run flaky tests sequentially
$ hatch run integration-tests-flaky

# Run tests in a specific module
$ hatch run integration-tests -- tests/functional/path/to/test_file.py

# Run a specific test class or method
$ hatch run integration-tests -- tests/functional/path/to/test_file.py::TestClass::test_method
```
