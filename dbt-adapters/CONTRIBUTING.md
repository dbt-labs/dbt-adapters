# Contributing to `dbt-adapters`

- [About this document](#about-this-document)
- [Developing](#developing)
- [Testing](#testing)

## About this document

This document covers contribution topics that specifically pertain to `dbt-adapters` (the package, not the repo).
Most topics are covered in the primary `CONTRIBUTING.md` doc; we expect this to develop over time.

## Developing

Make sure to always navigate to the `dbt-adapters` package subdirectory when working on this package.
Once you are in this directory, then things will behave as if this were not a monorepo.
For example, to initially create this package's virtual environment, run these commands:

```shell
cd dbt-adapters
hatch run setup
hatch shell
```

You will not need to run `cd dbt-adapters` every time you run a command.
But if you use an IDE such as PyCharm, you will likely need to run it each time you open a fresh terminal.

## Testing

The `dbt-adapters` package is subject to the general
[code quality checks](https://github.com/dbt-labs/dbt-adapters/tree/main/.pre-commit-config.yaml).
Additionally, there are [unit tests](https://github.com/dbt-labs/dbt-adapters/tree/main/tests/unit)
that are specific to `dbt-adapters`. There are no functional tests as these would require a database.
