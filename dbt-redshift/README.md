<p align="center">
    <img
        src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg"
        alt="dbt logo"
        width="500"
    />
</p>

<p align="center">
    <a href="https://pypi.org/project/dbt-redshift/">
        <img src="https://badge.fury.io/py/dbt-redshift.svg" />
    </a>
    <a target="_blank" href="https://pypi.org/project/dbt-redshift/" style="background:none">
        <img src="https://img.shields.io/pypi/pyversions/dbt-redshift">
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" />
    </a>
    <a href="https://github.com/python/mypy">
        <img src="https://www.mypy-lang.org/static/mypy_badge.svg" />
    </a>
    <a href="https://pepy.tech/project/dbt-redshift">
        <img src="https://static.pepy.tech/badge/dbt-redshift/month" />
    </a>
</p>

# dbt

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-redshift

`dbt-redshift` enables dbt to work with Amazon Redshift.
For more information on using dbt with Redshift, consult [the docs](https://docs.getdbt.com/docs/profile-redshift).

## ⚠️ This is a Fork

This repository is a fork of the original [dbt-redshift](https://github.com/dbt-labs/dbt-adapters) with additional SSO authentication support in dbt-redshift.

### Versioning of the Forked Packages

To maintain compatibility with both Semantic Versioning (SemVer) and PEP 440, and to clearly indicate the relationship with the original package versions, the versioning scheme for this fork follows a modified pattern:

- Versions based on original releases from the `1.9.5` series will use a major version of `19` to signify the fork, followed by the original minor and patch components. For example:
  - An original `1.9.5` release corresponds to `19.5.0` in this fork.
  - Subsequent patch releases on this fork will increment the last digit, e.g., `19.5.1`, `19.5.2`, etc.

- When the upstream package updates to eg. `1.10.1`, this fork will correspondingly update to a major version `110` to maintain clear upstream version alignment, for example:
  - `1.10.1` upstream → `110.1.0` forked version
  - Further patch releases would increment, e.g., `110.1.1`, `110.1.2`, etc.

This versioning approach avoids collision with expected upstream versions, provides clarity about the base upstream version, and remains fully compliant with both SemVer and PEP 440 standards.

### ✨ Additional Features

- **Single Sign-On (SSO) Authentication**: Support for OAuth2-based authentication using Azure AD and other identity providers
- **Token Management**: Automatic token refresh and caching for SSO sessions

# Getting started

Review the repository [README.md](../README.md) as most of that information pertains to `dbt-redshift`.

## Contribute

- Want to help us build `dbt-redshift`? Check out the [Contributing Guide](CONTRIBUTING.md).
