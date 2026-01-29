# this image gets published to GHCR for production use
ARG py_version=3.12.4

FROM python:$py_version-slim-bullseye AS base

RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
        build-essential=12.9 \
        ca-certificates=20210119 \
        git=1:2.30.2-1+deb11u2 \
        libpq-dev=13.18-0+deb11u1 \
        openssh-client=1:8.4p1-5+deb11u3 \
        software-properties-common=0.96.20.2-2.1 \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir


FROM base AS dbt-bigquery

ARG commit_ref=main

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-bigquery==${version}"


FROM base AS dbt-postgres

ARG version

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-postgres==${version}"


FROM base AS dbt-redshift

ARG commit_ref=main

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-redshift==${version}"


FROM base AS dbt-snowflake

ARG version

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-snowflake==${version}"


FROM base AS dbt-spark

ARG commit_ref=main
ARG extras=all

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
        gcc=4:10.2.1-1 \
        libsasl2-dev=2.1.27+dfsg-2.1+deb11u1 \
        python-dev-is-python2=2.7.18-9 \
        unixodbc-dev=2.3.6-0.1+b1 \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/*

RUN python -m pip install --no-cache-dir "dbt-spark[${extras}]==${version}"
