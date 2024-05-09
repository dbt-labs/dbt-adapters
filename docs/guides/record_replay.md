# Supporting Record/Replay in Adapters

This document describes how to implement support for dbt's Record/Replay Subsystem for adapters. Before reading it, make sure you understand the fundamental ideas behind Record/Replay, which are [documented in the dbt-common repo](https://github.com/dbt-labs/dbt-common/blob/docs/guides/record_replay.md).

## Recording and Replaying Warehouse Interaction

The goal of the Record/Replay Subsystem is to record all interactions between dbt and external systems, of which the data warehouse is the most obvious. Since, warehouse interaction is mediated by adapters, full Record/Replay support requires that adapters record all interactions they have with the warehouse. (It also requires that they record access to the local filesystem or external service, if that is access is not mediated by dbt itself. This includes authentication steps, opening and closing connections, beginning and ending transactions, and so forth.)

In practice, this means that any request sent to the warehouse must be recorded, along with the corresponding response. If this is done correctly, as described in the document linked in the intro, the Record portion of the Record/Replay subsystem should work as expected.

At the time of this writing, there is only an incomplete implementation of this goal, which can be found in `dbt-adapters/dbt/adapters/record.py`.

There are some iportant things to notice about this implementation. First, the QueryRecordResult class provides custom serialization methods `to_dict()` and `from_dict()`. This is necessary because the AdapterResponse and Agate.Table types cannot be automatically converted to and from JSON by the dataclass library, and JSON is the format used to persist recordings to disk and reload them for replay.

Another important feature is that QueryRecordParams, implements the `_matches()` method. This method allows dbt-adapters to customize the way that the Record/Replay determines whether a query issued by dbt matches a previously recorded query. In this case, the method performs a comparison which attempts to ignore comments and whitespace which would not affect query behavior.

## Misc. Notes and Suggestsions

Currently, support for recording data warehouse interaction is very rudimentary, however, even rudimentary support is valuable and we should concentrating on extending it in a way that adds the most value with the least work. Usefulness, rather than perfection, is the initial goal.

Picking the right functions to record, at the right level of abstraction, will probably be the most important part of carrying this work forward.

Not every interaction with an external system has to be recorded in full detail, and authentication might prove to be a place where exclude sensitive secrets from the recording. For example, since replay will not actually be communicating with the warehouse, it may be possible to exclude passwords and auth keys from the parameters recorded, and to exclude auth tokens from the results.

In addition to adding an appropriate decorator to functions which communicate with external systems, you should check those functions for side-effects. Since the function's calls will be mocked out in replay mode, those side-effects will not be carried out during replay. At present, we are focusing on support for recording and comparing recordings, but this is worth keeping in mind.

The current implementation records which dbt node issues a query, and uses that information to ensure a match during replay. The same node should issue the same query. A better model might be to monitor which connection issued which query, and associate the same connection with open/close operations, transaction starts/stops and so forth.


