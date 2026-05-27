<p align="center">
    <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>
<p align="center">
    <a href="https://github.com/dbt-labs/dbt-adapters/actions/workflows/scheduled-tests.yml">
        <img src="https://github.com/dbt-labs/dbt-adapters/actions/workflows/scheduled-tests.yml/badge.svg?event=schedule" alt="Scheduled tests badge"/>
    </a>
</p>

# dbt

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

### Upcoming Minor Releases

The following milestones track features that we're planning for the next minor version release of each adapter:

- [dbt-athena v1.11.0](https://github.com/dbt-labs/dbt-adapters/milestone/3) (2 PRs)
- [dbt-bigquery v1.12.0](https://github.com/dbt-labs/dbt-adapters/milestone/4) (31 PRs)
- [dbt-postgres v1.11.0](https://github.com/dbt-labs/dbt-adapters/milestone/5) (6 PRs)
- [dbt-redshift v1.11.0](https://github.com/dbt-labs/dbt-adapters/milestone/2) (24 PRs)
- [dbt-snowflake v1.12.0](https://github.com/dbt-labs/dbt-adapters/milestone/6) (23 PRs)
- [dbt-spark v1.11.0](https://github.com/dbt-labs/dbt-adapters/milestone/7) (2 PRs)

**Note:** PRs in these milestones may have been merged to `main` but not yet been promoted to the `stable` branch for patch releases in the current minor version.

# Getting started

## Install dbt

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/)
- Read the [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Suggest a feature or report a bug

- Submit a bug or a feature as a GitHub [issue](https://github.com/dbt-labs/dbt-adapters/issues/new/choose)

## Contribute

- Want to help us build dbt? Check out the [Contributing Guide](CONTRIBUTING.md)

# Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):
