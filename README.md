<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

# dbt-tests-adapter

This package is responsible for:

- defining database connection methods
- caching information from databases
- determining how relations are defined

There are two major adapter types: base and sql

# Directories

## `base`

Defines the base implementation Adapters can use to build out full functionality.

## `sql`

Defines a sql implementation for adapters that initially inherits the base implementation
and comes with some pre-made methods and macros that can be overwritten as needed per adapter.
(most common type of adapter.)

# Files

## `cache.py`

Cached information from the database.

## `factory.py`

Defines how we generate adapter objects

## `protocol.py`

Defines various interfaces for various adapter objects. Helps mypy correctly resolve methods.

## `reference_keys.py`

Configures naming scheme for cache elements to be universal.
