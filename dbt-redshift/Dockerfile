# this image does not get published, it is intended for local development only, see `Makefile` for usage
FROM ubuntu:24.04 as base

# prevent python installation from asking for time zone region
ARG DEBIAN_FRONTEND=noninteractive

# add python repository
RUN apt-get update \
    && apt-get install -y software-properties-common=0.99.48 \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/*

# install python
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential=12.10ubuntu1 \
        git-all=1:2.43.0-1ubuntu7.1 \
        python3.10=3.10.19-1+noble1 \
        python3.10-dev=3.10.19-1+noble1 \
        python3.10-distutils=3.10.19-1+noble1 \
        python3.10-venv=3.10.19-1+noble1 \
        python3-pip=24.0+dfsg-1ubuntu1 \
        python3-wheel=0.42.0-2 \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/*

# update the default system interpreter to the newly installed version
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1


FROM base as dbt-redshift-dev

HEALTHCHECK CMD python --version || exit 1

# send stdout/stderr to terminal
ENV PYTHONUNBUFFERED=1

# setup mount for local code
WORKDIR /opt/code
VOLUME /opt/code

# create a virtual environment
RUN python3 -m venv /opt/venv
