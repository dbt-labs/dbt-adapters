#!/bin/bash

set -eo pipefail
adapter=$1
branch=$2
source .hatch/test.env
adapter_dir="dbt-$adapter"

cd .hatch/
if [ ! -d "$adapter_dir" ]; then
  git clone "https://github.com/dbt-labs/dbt-$adapter.git"
fi

cd "$adapter_dir"
git pull
git switch "$branch"
pip install -e .
python -m pytest tests/functional/
