# Contributing to `dbt-bigquery`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## Integration tests

Integration tests require access to a Google Cloud Platform project with BigQuery enabled and a service account with appropriate BigQuery permissions.

### Configure environment variables

Running `hatch run setup` will create a `test.env` file from `test.env.example` in this package directory if one does not already exist. Update `test.env` with your GCP details:

```bash
# Service account JSON key contents
BIGQUERY_TEST_SERVICE_ACCOUNT_JSON='{"type": "service_account", ...}'

# A GCP project you don't have access to query (for negative permission tests)
BIGQUERY_TEST_NO_ACCESS_DATABASE=<inaccessible-project>

# Optional: GCS bucket with write access for Iceberg / staging tests
BIGQUERY_TEST_ICEBERG_BUCKET=<bucket-name>
```

> [!WARNING]
> Never commit `test.env` or any file containing live credentials to git.

### Run integration tests

Run integration tests from within the `dbt-bigquery` directory:

```shell
# Run all non-flaky integration tests
$ hatch run integration-tests

# Run flaky tests
$ hatch run integration-tests-flaky

# Run tests in a specific module
$ hatch run integration-tests -- tests/functional/path/to/test_file.py

# Run a specific test class or method
$ hatch run integration-tests -- tests/functional/path/to/test_file.py::TestClass::test_method
```
