# Contributing to `dbt-athena`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

## Integration tests

Integration tests require access to AWS with permissions for Athena, AWS Glue Data Catalog, and Amazon S3.

### Configure environment variables

Running `hatch run setup` will create a `test.env` file from `test.env.example` in this package directory if one does not already exist. Update `test.env` with your AWS and Athena configuration:

```bash
DBT_TEST_ATHENA_S3_STAGING_DIR=s3://<staging-bucket>/<prefix>/
DBT_TEST_ATHENA_S3_TMP_TABLE_DIR=s3://<tmp-bucket>/<prefix>/
DBT_TEST_ATHENA_REGION_NAME=<aws-region>
DBT_TEST_ATHENA_DATABASE=<glue_database>
DBT_TEST_ATHENA_SCHEMA=<glue_database>
DBT_TEST_ATHENA_WORK_GROUP=<workgroup>
DBT_TEST_ATHENA_AWS_PROFILE_NAME=<optional_aws_profile>
```

> [!WARNING]
> Never commit `test.env` or any file containing live credentials to git.

### Run integration tests

Run integration tests from within the `dbt-athena` directory:

```shell
# Run all integration tests
$ hatch run integration-tests

# Run tests in a specific module
$ hatch run integration-tests -- tests/functional/path/to/test_file.py

# Run a specific test class or method
$ hatch run integration-tests -- tests/functional/path/to/test_file.py::TestClass::test_method
```
