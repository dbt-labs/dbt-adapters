<p align="center">
    <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>
<p align="center">
    <a href="https://github.com/dbt-labs/dbt-adapters/actions/workflows/scheduled-tests.yml">
        <img src="https://github.com/dbt-labs/dbt-adapters/actions/workflows/scheduled-tests.yml/badge.svg?event=schedule" alt="Scheduled tests badge"/>
    </a>
</p>

# dbt

> [!NOTE]
> This repository hosts the **v1** generation of dbt adapters. The **v2** adapters are developed in the [dbt-core](https://github.com/dbt-labs/dbt-core) repository.

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## Adapters

This repository is a monorepo containing the following packages:

- Base adapter:
  - [dbt-adapters](/dbt-adapters)
- Adapter integration test suite:
  - [dbt-tests-adapter](/dbt-tests-adapter)
- First party adapters:
  - [dbt-athena](/dbt-athena)
  - [dbt-bigquery](/dbt-bigquery)
  - [dbt-postgres](/dbt-postgres)
  - [dbt-redshift](/dbt-redshift)
  - [dbt-snowflake](/dbt-snowflake)
  - [dbt-spark](/dbt-spark)

Please refer to each of these packages for more specific information.

## Releases

All of our packages are merged off of main except for `dbt-adapters` and `dbt-tests-adapter`. Therefore merging a pull request to main does not automatically put it in the queue for the next release. To do so please add the 'promote to stable' label to the PR once it's been merged.

The reason we do this is to allow us to patch the previous minor version with updates (i.e. what's in stable) as needed while preparing what's on main (the next minor release) to be ready for release.

# Getting started

## Install dbt

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/)
- Read the [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Report a bug or suggest a feature

- Report a bug in a dbt v1.x adapter as a GitHub [issue](https://github.com/dbt-labs/dbt-adapters/issues/new/choose)
- dbt v1.x adapters are no longer accepting new features. New adapter features are only being added to dbt v2.x; propose them, or report dbt v2.x bugs, in [dbt-labs/dbt](https://github.com/dbt-labs/dbt/issues/new/choose)

## Contribute

- Want to help us build dbt? Check out the [Contributing Guide](CONTRIBUTING.md)

# Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):
