# Supporting Record/Replay in Adapters

This document describes how to implement support for dbt's Record/Replay Subsystem for adapters. Before reading it, make sure you understand the fundamental ideas behind Record/Replay, which are [documented in the dbt-common repo](https://github.com/dbt-labs/dbt-common/blob/docs/guides/record_replay.md).

## Recording and Replaying Warehouse Interaction

The goal of the Record/Replay Subsystem is to record all interactions between dbt and external systems, of which the data warehouse is the most important. Since, warehouse interaction is mediated by adapters, full Record/Replay support requires that adapters record all interactions they have with the warehouse. It also requires that they record access to the local filesystem or external service, if that is access is not mediated by dbt itself. This includes authentication steps, opening and closing connections, beginning and ending transactions, etc.

A basic implementation of Record/Replay functionality, suitable for most adapters which extend the `SQLAdapter` class, can be found in `dbt-adapters/dbt/adapters/record`. The `RecordReplayHandle` and `RecordReplayCursor` classes defined there are used to intercept and record or replay all DWH interactions. They are an excellent starting point for adapters which extend `SQLAdapter` and use a database library which substantially conforms to Python's DB API v2.0 (PEP 249). Examples of how library-specific deviations from that API can be found in the dbt-postgress and dbt-snowflake repositories.

## Misc. Notes and Suggestions

Not every interaction with an external system has to be recorded in full detail, and authentication might prove to be a place where we exclude sensitive secrets from the recording. For example, since replay will not actually be communicating with the warehouse, it may be possible to exclude passwords and auth keys from the parameters recorded, and to exclude auth tokens from the results.

In addition to adding an appropriate decorator to functions which communicate with external systems, you should check those functions for side-effects. Since the function's calls will be mocked out in replay mode, those side-effects will not be carried out during replay. At present, we are focusing on support for recording and comparing recordings, but this is worth keeping in mind.
