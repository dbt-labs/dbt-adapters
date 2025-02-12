<p align="center">
    <img
        src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg"
        alt="dbt logo"
        width="500"
    />
</p>

<p align="center">
    <a href="https://pypi.org/project/dbt-postgres/">
        <img src="https://badge.fury.io/py/dbt-postgres.svg" />
    </a>
    <a target="_blank" href="https://pypi.org/project/dbt-postgres/" style="background:none">
        <img src="https://img.shields.io/pypi/pyversions/dbt-postgres">
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" />
    </a>
    <a href="https://github.com/python/mypy">
        <img src="https://www.mypy-lang.org/static/mypy_badge.svg" />
    </a>
    <a href="https://pepy.tech/project/dbt-postgres">
        <img src="https://static.pepy.tech/badge/dbt-postgres/month" />
    </a>
</p>

# dbt

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-postgres

`dbt-postgres` enables dbt to work with Postgres.
For more information on using dbt with Postgres, consult [the docs](https://docs.getdbt.com/docs/profile-postgres).

# Getting started

Review the repository [README.md](../README.md) as most of that information pertains to `dbt-postgres`.

### psycopg2-binary vs. psycopg2

By default, `dbt-postgres` installs `psycopg2-binary`.
This is great for development, and even testing, as it does not require any OS dependencies; it's a pre-built wheel.
However, building `psycopg2` from source will grant performance improvements that are desired in a production environment.
In order to install `psycopg2`, use the following steps:

```bash
if [[ $(pip show psycopg2-binary) ]]; then
    PSYCOPG2_VERSION=$(pip show psycopg2-binary | grep Version | cut -d " " -f 2)
    pip uninstall -y psycopg2-binary
    pip install psycopg2==$PSYCOPG2_VERSION
fi
```

This ensures the version of `psycopg2` will match that of `psycopg2-binary`.

## Contribute

- Want to help us build `dbt-postgres`? Check out the [Contributing Guide](CONTRIBUTING.md).
