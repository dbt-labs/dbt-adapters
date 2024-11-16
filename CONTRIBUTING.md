# Contributing

- [About this document](#about-this-document)
- [Getting the code](#getting-the-code)
- [Developing](#developing)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting a pull request](#submitting-a-pull-request)

## About this document

This document is a guide for anyone interested in contributing to this repository.
This repository is a collection of packages, each with their own nuances;
however, all package share significant overlap, which is covered here.
For more information on how to contribute to a specific package,
please refer to that package's `CONTRIBUTING.md`.

This document outlines how to install a package for development,
run tests locally, update documentation, and submit pull requests.
This guide assumes users are developing on a Linux or MacOS system.
The following utilities are needed or will be installed in this guide:

- `pip`
- `hatch`
- `git`
- `changie`

In addition to this guide, users are highly encouraged to read the `dbt-core`
[CONTRIBUTING.md](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md).
Almost all information there is applicable here.

## Getting the code

`git` is required to download, modify, and sync code.
There are several ways to install Git. For MacOS:

- Install [Xcode](https://developer.apple.com/support/xcode/)
- Install [Xcode Command Line Tools](https://mac.install.guide/commandlinetools/index.html)

### External contributors

Contributors external to the `dbt-labs` GitHub organization can contribute to this repository
by forking the `dbt-adapters` repository. For more on forking, check out the
[GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). To contribute:

1. Fork the `dbt-labs/dbt-adapters` repository (e.g. `{forked-org}/dbt-adapters`)
2. Clone `{forked-org}/dbt-adapters` locally
3. Check out a new branch locally
4. Make changes in the new branch
5. Push the new branch to `{forked-org}/dbt-adapters`
6. Open a pull request in `dbt-labs/dbt-adapters` to merge `{forked-org}/dbt-adapters/{new-branch}` into `main`

### dbt Labs contributors

Contributors in the `dbt Labs` GitHub organization have push access to the `dbt-adapters` repo.
Rather than forking `dbt-labs/dbt-adapters`, use `dbt-labs/dbt-adapters` directly. To contribute:

1. Clone `dbt-labs/dbt-adapters` locally
2. Check out a new branch locally
3. Make changes in the new branch
4. Push the new branch to `dbt-labs/dbt-adapters`
5. Open a pull request in `dbt-labs/dbt-adapters` to merge `{new-branch}` into `main`

## Developing

### Installation

1. Ensure the latest versions of `pip` and `hatch` are installed:
   ```shell
   pip install --user --upgrade pip hatch
   ```
2. This step is optional, but it's recommended. Configure `hatch` to create its virtual environments in the project.
Add this block to your `hatch` `config.toml` file:
   ```toml
   # MacOS: ~/Library/Application Support/hatch/config.toml
   [dirs.env]
   virtual = ".hatch"
   ```
   This makes `hatch` create all virtual environments in the project root inside of the directory `/.hatch`, similar to `/.tox` for `tox`.
   It also makes it easier to add this environment as a runner in common IDEs like VSCode and PyCharm.
3. Navigate to the package that you are working on. Note that because this repository is a monorepo,
you will have multiple packages, each with their own set of virtual environments.
   ```shell
   ls
   > .github dbt-adapters dbt-tests-adapter docs third-party-stubs .changie.yml .gitignore ...

   cd dbt-adapters

   ls
   > .changes src tests CHANGELOG.md CONTRIBUTING.md pyproject.toml README.md
   ```
4. Create a `hatch` environment with all of the development dependencies and activate it:
   ```shell
   hatch run setup
   hatch shell
   ```
4. Run any commands within the virtual environment by prefixing the command with `hatch run`:
   ```shell
   hatch run <command>
   ```

## Testing

The `dbt-adapters` repo contains
[code quality checks](https://github.com/dbt-labs/dbt-adapters/tree/main/.pre-commit-config.yaml)
that will run across the entire repo.
Additionally, each package has their own unit tests and functional tests. See your particular
package's `CONTRIBUTING.md` for more information regarding these tests.

### Code quality

Code quality checks can run with a single command:
```shell
hatch run code-quality
```

### Unit tests

Unit tests can be run locally without setting up a database connection:

```shell
# Note:
#   - replace $strings with valid names
#   - ensure that you are in the project directory, and not the repo root

# run all unit tests
hatch run unit-test

# run all unit tests in a module
hatch run unit-test tests/unit/$test_file_name.py

# run a specific unit test
hatch run unit-test tests/unit/$test_file_name.py::$test_class_name::$test_method_name
```

### Testing against a development branch

Some changes require a change in `dbt-common` and `dbt-adapters`.
In that case, the dependency on `dbt-common` must be updated to point to the development branch. For example:

```toml
[tool.hatch.envs.default]
dependencies = [
    "dbt-common @ git+https://github.com/dbt-labs/dbt-common.git@my-dev-branch",
    ...,
]
```

This will install `dbt-common` as a snapshot. In other words, if `my-dev-branch` is updated on GitHub, those updates will not be reflected locally.
In order to pick up those updates, the `hatch` environment(s) will need to be rebuilt:

```shell
exit
hatch env prune
hatch shell
```

## Documentation

### User documentation

Many changes will require an update to `dbt-adapters` user documentation.
All contributors, whether internal or external, are encouraged to open an issue or PR
in the docs repo when submitting user-facing changes. Here are some relevant links:

- [User docs](https://docs.getdbt.com/)
  - [Warehouse Profile](https://docs.getdbt.com/reference/warehouse-profiles/)
  - [Resource Configs](https://docs.getdbt.com/reference/resource-configs/)
- [User docs repo](https://github.com/dbt-labs/docs.getdbt.com)

### CHANGELOG entry

`dbt-adapters` uses [changie](https://changie.dev) to generate `CHANGELOG` entries.
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

> **_NOTE:_** All contributors to `dbt-adapters` must sign the
> [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements)(CLA).

Maintainers will be unable to merge contributions until the contributor signs the CLA.
This is a one time requirement, not a per-PR requirement.
Even without a CLA, anyone is welcome to open issues and comment on existing issues or PRs.

### Opening a pull request

A `dbt-adapters` maintainer will be assigned to review each PR based on priority and capacity.
They may suggest code revisions for style and clarity or they may request additional tests.
These are good things! dbt Labs believes that contributing high-quality code is a collaborative effort.
The same process is followed whether the contributor is external or another `dbt-adapters` maintainer.
Once all tests are passing and the PR has been approved by the appropriate code owners,
a `dbt-adapters` maintainer will merge the changes into `main`.

And that's it! Happy developing :tada:
