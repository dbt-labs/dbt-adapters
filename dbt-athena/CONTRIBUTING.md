# Contributing to `dbt-athena`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

## Integration tests

To run integration tests of `dbt-athena` locally, you need an AWS account with access to Athena, Glue Catalog, and S3.

For general guidance, refer to the main [CONTRIBUTING.md](/CONTRIBUTING.md#integration-tests).

### Configure environment variables

Refer to the main [CONTRIBUTING.md](/CONTRIBUTING.md#configure-environment-variables).

The configuration template [`test.env.example`](test.env.example) is copied to `test.env` when you run `hatch run setup`.

Customize the values in `test.env` according to your preferred setup.
