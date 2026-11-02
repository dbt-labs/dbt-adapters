# this image gets published to GHCR for production use
ARG py_version=3.11.9

FROM python:$py_version-slim-bookworm AS base

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    build-essential=12.9 \
    ca-certificates=20250419~deb12u2 \
    git=1:2.39.5-0+deb12u3 \
    libpq-dev=15.19-0+deb12u1 \
    make=4.3-4.1 \
    openssh-client=1:9.2p1-2+deb12u10 \
    software-properties-common=0.99.30-4.1~deb12u1 \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade "pip==24.0" "setuptools==69.2.0" "wheel==0.43.0" --no-cache-dir


FROM base AS dbt-bigquery

ARG commit_ref=main

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-bigquery @ git+https://github.com/dbt-labs/dbt-adapters@${commit_ref}#subdirectory=dbt-bigquery"
