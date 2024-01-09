# Adapter

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
