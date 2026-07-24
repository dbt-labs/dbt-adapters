# Contributing

This document covers the following topics:

- [How to install a package for development](#install-a-package-for-development)
- [How to run code quality tests](#code-quality-checks)
- [How to run unit tests](#unit-tests)
- [How to run integration tests](#integration-tests)
- [How to submit a pull request](#submitting-a-pull-request)
- [How to update documentation](#documentation)

> [!TIP]
> Visit [Become a contributor](https://docs.getdbt.com/community/contribute) to learn more about contributing at dbt!

The following utilities are needed for developing the packages in this repository:

- `pip`
- `hatch`
- `git`
- `changie`
- `pre-commit`

> [!NOTE]
> External contributors can contribute to this repository by forking the `dbt-labs/dbt-adapters` repository.
> For more on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo).

# Developing

This repository is a monorepo, meaning that it contains several packages that can each be installed independently.
This allows for easier testing of related changes that live in different packages.
For example, say you want to implement a feature in `dbt-postgres` that requires an update to `dbt-adapters`.
This requires a change to at least two (`dbt-tests-adapter`?) different packages.
This differs from a more traditional approach where each repository contains a single package.
In particular, everything is moved "one level down".
Make sure to take this into account when working with a specific package.

## Install a package for development

> [!IMPORTANT]
> Ensure that you're in a package directory prior to running any commands for a specific package, e.g.:
> ```shell
> cd dbt-postgres
> ```

### Create a `hatch` environment

```shell
$ hatch run setup
```
- Installs the package in editable mode
- Installs development dependencies
- Installs all repository packages in editable mode (e.g. `dbt-adapters` and `dbt-tests-adapter`)
- Installs `pre-commit` hooks
- Creates a templated `test.env` from `test.env.example` (if `test.env` doesn't already exist)

> [!TIP]
> Configure `hatch` to create virtual environments in the package directory:
> ```shell
> hatch config set dirs.env.virtual .hatch
> ```
> This has two main benefits:
> 1. It's easier to find the virtual environment to add it as a runner in common IDEs like VSCode and PyCharm
> 2. It names the environment predictable (e.g. `default`, `build`) instead of using a hash

### Activate a `hatch` environment

```shell
$ hatch shell
(default) $
```

### Run a command in a `hatch` environment

```shell
$ hatch run <command>
```
This is effectively shorthand for:
```shell
$ hatch shell
(default) $ <command>
```

### Rebuild a `hatch` environment

```shell
# exit first if you're still in the hatch environment
(default) $ exit
$ hatch env prune
$ hatch shell
```

### Developing against feature branches

This repository is setup to install repository packages in editable mode.
For example, any local changes in `dbt-adapters` will be reflected in the `dbt-postgres` virtual environment.
This will also make pull request checks work as expected without needing to alter development requirements to point to a feature branch.
However, this does not work with packages outside of the repository as there is no guarantee of where they are.

Some changes require a change in `dbt-common` and/or `dbt-core`.
In that case, `dbt-common` (or `dbt-core`) must be installed from that a version of branch.
The best way to do this is to re-install `dbt-common` into your `hatch` environment in editable mode:

```shell
$ hatch run pip install -e ../../dbt-common
```

> [!NOTE]
> Note that there are two `../` above.
> The first corresponds to this repository's root, and the second to your development space, e.g. `~/Source`.
> This assumes that `{username}/dbt-common` is cloned alongside `{username}/dbt-adapters`.

> [!IMPORTANT]
> This command will need to be re-run every time the `hatch` environment is rebuilt.

You will also need to point CI to this feature branch if you are using this to test your changes.
The only way to do this is to update `hatch.toml` to point the dependency at your feature branch:

```toml
# {adapter}/hatch.toml
[envs.default]
dependencies = [
    "dbt-common @ git+https://github.com/{username}/dbt-common.git@{branch}",
    "...",
]
```

> [!IMPORTANT]
> This update to `hatch.toml` will need to be reverted prior to merging into `main`.

# Testing

Tests in this repository can be broken up into three categories:
- [Code quality checks](#code-quality-checks)
- [Unit tests](#unit-tests)
- [Integration tests](#integration-tests)

## Code quality checks

Code quality checks are [configured](.pre-commit-config.yaml) at the repo level.
These checks can run from a virtual environment or by invoking `pre-commit` directly:

```shell
$ hatch run code-quality
```
OR
```shell
$ pre-commit run --all-files
```

## Unit tests

Unit tests can be run locally without setting up a database connection.
Unit tests need to be run from a package directory and will not work at the repo root directory.

```shell
# run all unit tests
$ hatch run unit-tests

# run all unit tests in a module
$ hatch run unit-tests tests/unit/{test_file_name}.py

# run a specific unit test
$ hatch run unit-tests tests/unit/{test_file_name}.py::{test_class_name}::{test_method_name}
```

## Integration tests

Integration tests require setting up a database connection to run locally.
This will vary by package; please refer to the package's `CONTRIBUTING.md` for more information.

### Configure environment variables

Each adapter requires certain environment variables to connect to its platform.
The template is contained in the respective `test.env.example` file.
If you already ran `hatch run setup` you should have a `test.env` file in the package root.
Update the environment variables in this file with your instance's connection credentials.

### Run integration tests

```shell
# run all integration tests
$ hatch run integration-tests
```

# Documentation

## Release documentation

This repository uses [changie](https://changie.dev) to generate `CHANGELOG` entries.
These entries get collated and injected into `CHANGELOG.md` at release, so there's no need to update `CHANGELOG.md` directly.
Follow the steps to [install `changie`](https://changie.dev/guide/installation/).

Once `changie` is installed and the PR is created, run:

```shell
$ changie new
```

`changie` will walk through the process of creating a changelog entry.
Remember to commit and push the file that's created.

> [!IMPORTANT]
> Do not edit the `CHANGELOG.md` directly.
> Any modifications will be lost by the consolidation process.

## User documentation

Many changes will require an update to [user documentation](https://docs.getdbt.com/).
All contributors, whether internal or external, are encouraged to open an issue and/or pull request
in the [docs repo](https://github.com/dbt-labs/docs.getdbt.com) when submitting user-facing changes.
Here are some relevant links when considering what to update:

- [User docs](https://docs.getdbt.com/)
  - [Warehouse Profile](https://docs.getdbt.com/reference/warehouse-profiles/)
  - [Resource Configs](https://docs.getdbt.com/reference/resource-configs/)

# Submitting a pull request

## Communication

Please refer to our [Code of Conduct](/CODE_OF_CONDUCT.md) when opening issues and pull requests.

> [!NOTE]
> We value and appreciate all contributions from our community.
> To ensure an organized and efficient process, we kindly ask that you refrain from tagging dbt Labs employees to request features, report bugs, or seek PR reviews.
> Our [community guidelines](https://docs.getdbt.com/community/resources/community-rules-of-the-road) also extend to all of our repositories.
> Please note that we have an established triage process and will respond to issues as soon as possible.
> If you are a dbt Cloud user, we request that you submit issues directly to our Support Team to ensure proper tracking and timely follow-up.

## Signing the CLA

> [!IMPORTANT]
> All contributors must sign the
> [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements)(CLA).

Maintainers will be unable to merge contributions until the contributor signs the CLA.
This is a one time requirement, not a per-PR or per-repo requirement.
Even without a CLA, anyone is welcome to open issues and comment on existing issues or pull requests.

## Opening a pull request

A maintainer will be assigned to review each pull request based on priority and capacity.
They may suggest code revisions for style and clarity or they may request additional tests.
These are good things! dbt Labs believes that contributing high-quality code is a collaborative effort.
The same process is followed whether the contributor is external or a maintainer.
Once all tests are passing and the pull request has been approved by the appropriate code owners,
a maintainer will merge the changes into `main`.

### Migrating an existing pull request into the monorepo

You may have opened your pull request against an adapter prior to its migration into the monorepo.
In that case, you'll need to migrate that pull request into this repo.
Unfortunately, GitHub does not allow you to migrate a pull request; however, you can migrate your branch.
Here are the steps to do that:

1. Fork this repository if you haven't already and pull it down locally
2. Run the script `./scripts/migrate-branch.sh` (replacing the args with your versions):
   ```shell
   source ./scripts/migrate-branch.sh dbt-labs dbt-postgres my-cool-feature-branch
   ```
3. Push your new feature branch back up to your fork of this repository
4. Open up a new pull request into `dbt-adapters` from your fork

And that's it! Happy developing :tada:
