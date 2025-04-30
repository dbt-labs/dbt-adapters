# Contributing to `dbt-adapters`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

# Testing

`dbt-adapters` differs from most other packages in this repository.
Since `dbt-adapters` is the base adapter, it does not connect to a database, hence contains no integration tests.
