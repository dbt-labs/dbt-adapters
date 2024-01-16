# Build Tool


## Context

We need to select a build tool for managing dependencies for, building, and distributing adapters,
including `dbt-adapters`. While build tools can change for adapters that inherit from
`dbt-adapters`, this repo also serves as a template for many third-party adapters.


## Options

- `setuptools` (`twine`, `build`)
- `hatch`
- `poetry`


### setuptools

#### Pro's

- most popular option
- supported by Python Packaging Authority
- build tool of record for existing internal adapters

#### Con's

- less flexible; forced to support backwards compatibility more so than other options
- no dependency management (manually add to `pyproject.toml`)


### hatch

#### Pro's

- supported by Python Packaging Authority
- build tool for next-gen related packages (e.g. dbt-common, dbt-semantic-layer)
- supports running tests against multiple versions of python locally (`tox`)
- supports configuring workflows in `pyrpoject.toml` (`make`)
- incorporates new PEP's quickly

#### Con's

- far less popular than other options
- no dependency management (manually add to `pyproject.toml`)
- only one maintainer


### poetry

#### Pro's

- second most popular option, similar in popularity to `setuptools`
- dependency management (`poetry add "my-dependency"`)
- provides a lock file
- more than one maintainer

#### Con's

- incorporates new PEP's slowly


## Decision

#### Selected: `hatch`

This option aligns with `dbt-common` and `dbt-semantic-layer`, which minimizes confusion
for both internal maintainers and third party adapter maintainers.
`hatch` also replaces `tox` and `make`, which consolidates our toolset.


## Consequences

- [+] retire `tox`
- [+] retire `make`
- [-] write more detailed docs given lower familiarity
- [-] learning curve
