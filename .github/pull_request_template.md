resolves #
[docs](https://github.com/dbt-labs/docs.getdbt.com/issues/new/choose) dbt-labs/docs.getdbt.com/#

<!---
  Include the number of the issue addressed by this PR above if applicable.
  PRs for code changes without an associated issue *will not be merged*.
  See CONTRIBUTING.md for more information.

  Include the number of the docs issue that was opened for this PR. If
  this change has no user-facing implications, "N/A" suffices instead. New
  docs tickets can be created by clicking the link above or by going to
  https://github.com/dbt-labs/docs.getdbt.com/issues/new/choose.
-->

### Problem

<!---
  Describe the problem this PR is solving. What is the application state
  before this PR is merged?
-->

### Solution

<!---
  Describe the way this PR solves the above problem. Add as much detail as you
  can to help reviewers understand your changes. Include any alternatives and
  tradeoffs you considered.
-->

### Concrete Adapter Testing

At the appropriate stage of development or review, please use an integration test workflow in each of the following repos against your branch.

Use these to confirm that your feature add or bug fix (1) achieves the desired behavior (2) does not disrupt other concrete adapters:
* [ ] Postgres
* [ ] Snowflake
* [ ] Spark
* [ ] Redshift
* [ ] Bigquery

Please link to each CI invocation workflow in this checklist here or in a separate PR comment.

*Note*: Before hitting merge, best practice is to test against your PR's latest SHA.

### Checklist

- [ ] I have read [the contributing guide](https://github.com/dbt-labs/dbt-adapters/blob/main/CONTRIBUTING.md) and understand what's expected of me
- [ ] I have run this code in development, and it appears to resolve the stated issue
- [ ] This PR includes tests, or tests are not required/relevant for this PR
- [ ] This PR has no interface changes (e.g. macros, cli, logs, json artifacts, config files, adapter interface, etc.) or this PR has already received feedback and approval from Product or DX
