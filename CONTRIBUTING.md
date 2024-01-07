# Contributing to `dbt-adapter`

- [About this document](#about-this-document)
- [Getting the code](#getting-the-code)
- [Developing](#developing)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting a pull request](#submitting-a-pull-request)


## About this document

This document is a guide for anyone interested in contributing to `dbt-adapter`.
It outlines how to install `dbt-adapter` for development,
run tests locally, update documentation, and submit pull requests.
This guide assumes users are developing on a Linux or MacOS system.
The following utilities are needed or will be installed in this guide:

- `pip`
- `virturalenv`
- `git`
- `changie`

If local functional testing is required, then a database instance
and appropriate credentials are also required.

In addition to this guide, users are highly encouraged to read the `dbt-core`
[CONTRIBUTING.md](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md).
Almost all information there is applicable here.


## Getting the code

`git` is required to download, modify, and sync the `dbt-adapter` code.
There are several ways to install Git. For MacOS:

- Install [Xcode](https://developer.apple.com/support/xcode/)
- Install [Xcode Command Line Tools](https://mac.install.guide/commandlinetools/index.html)

### External contributors

Contributors external to the `dbt-labs` GitHub organization can contribute to `dbt-adapter`
by forking the `dbt-adapter` repository. For more on forking, check out the
[GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). To contribute:

1. Fork the `dbt-labs/dbt-adapter` repository (e.g. `{forked-org}/dbt-adapter`)
2. Clone `{forked-org}/dbt-adapter` locally
3. Check out a new branch locally
4. Make changes in the new branch
5. Push the new branch to `{forked-org}/dbt-adapter`
6. Open a pull request in `dbt-labs/dbt-adapter` to merge `{forked-org}/dbt-adapter/{new-branch}` into `main`

### dbt Labs contributors

Contributors in the `dbt Labs` GitHub organization have push access to the `dbt-adapter` repo.
Rather than forking `dbt-labs/dbt-adapter`, use `dbt-labs/dbt-adapter` directly. To contribute:

1. Clone `dbt-labs/dbt-adapter` locally
2. Check out a new branch locally
3. Make changes in the new branch
4. Push the new branch to `dbt-labs/dbt-adapter`
5. Open a pull request in `dbt-labs/dbt-adapter` to merge `{new-branch}` into `main`


## Developing

### Installation

1. Ensure the latest version of `pip` is installed:
   ```shell
   pip install --upgrade pip
   ```
2. Configure and activate a virtual environment using `virtualenv` as described in
[Setting up an environment](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md#setting-up-an-environment)
3. Install `dbt-adapter` and development dependencies in the virtual environment
   ```shell
   pip install -e .[dev]
   ```

When `dbt-adapter` is installed this way, any changes made to the `dbt-adapter` source code
will be reflected in the virtual environment immediately.


## Testing

`dbt-adapter` contains [unit](https://github.com/dbt-labs/dbt-adapter/tree/main/tests/unit)
and [functional](https://github.com/dbt-labs/dbt-adapter/tree/main/tests/functional) tests.

### Unit tests

Unit tests can be run locally without setting up a database connection:

```shell
# Note: replace $strings with valid names

# run all unit tests in a module
python -m pytest tests/unit/$test_file_name.py
# run a specific unit test
python -m pytest tests/unit/$test_file_name.py::$test_class_name::$test_method_name
```

### Functional tests

Functional tests require a database to test against. There are two primary ways to run functional tests:

- Tests will run automatically against a dbt Labs owned database during PR checks
- Tests can be run locally by configuring a `test.env` file with appropriate `ENV` variables:
   ```shell
   cp test.env.example test.env
   $EDITOR test.env
   ```

> **_WARNING:_** The parameters in `test.env` must link to a valid database.
> `test.env` is git-ignored, but be _extra_ careful to never check in credentials
> or other sensitive information when developing.

Functional tests can be run locally with a valid database connection configured in `test.env`:

```shell
# Note: replace $strings with valid names

# run all functional tests in a directory
python -m pytest tests/functional/$test_directory
# run all functional tests in a module
python -m pytest tests/functional/$test_dir_and_filename.py
# run all functional tests in a class
python -m pytest tests/functional/$test_dir_and_filename.py::$test_class_name
# run a specific functional test
python -m pytest tests/functional/$test_dir_and_filename.py::$test_class_name::$test__method_name
```


## Documentation

### User documentation

Many changes will require an update to `dbt-adapter` user documentation.
All contributors, whether internal or external, are encouraged to open an issue or PR
in the docs repo when submitting user-facing changes. Here are some relevant links:

- [User docs](https://docs.getdbt.com/)
  - [Warehouse Profile](https://docs.getdbt.com/reference/warehouse-profiles/)
  - [Resource Configs](https://docs.getdbt.com/reference/resource-configs/)
- [User docs repo](https://github.com/dbt-labs/docs.getdbt.com)

### CHANGELOG entry

`dbt-adapter` uses [changie](https://changie.dev) to generate `CHANGELOG` entries.
Follow the steps to [install `changie`](https://changie.dev/guide/installation/).

Once changie is installed and the PR is created, run:
   ```shell
   changie new
   ```
`changie` will walk through the process of creating a changelog entry.
Remember to commit and push the file that's created.

> **_NOTE:_** Do not edit the `CHANGELOG.md` directly.
> Any modifications will be lost by the consolidation process.


## Submitting a pull request

### Signing the CLA

> **_NOTE:_** All contributors to `dbt-adapter` must sign the 
> [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements)(CLA).

Maintainers will be unable to merge contributions until the contributor signs the CLA.
This is a one time requirement, not a per-PR requirement.
Even without a CLA, anyone is welcome to open issues and comment on existing issues or PRs.

### Opening a pull request

A `dbt-adapter` maintainer will be assigned to review each PR based on priority and capacity.
They may suggest code revisions for style and clarity or they may request additional tests.
These are good things! dbt Labs believes that contributing high-quality code is a collaborative effort.
The same process is followed whether the contributor is external or another `dbt-adapter` maintainer.
Once all tests are passing and the PR has been approved by the appropriate code owners,
a `dbt-adapter` maintainer will merge the changes into `main`.

And that's it! Happy developing :tada:
