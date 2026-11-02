#!/bin/bash -e
set -e

git_branch=$1

if [[ "${git_branch}" == "main" ]]; then
    echo "ERROR: branch cannot be 'main' — this value is used as the dbt-core ref. Use '1.latest' or a specific version branch (e.g. '1.10.latest')." >&2
    exit 1
fi

target_req_file="hatch.toml"
core_req_sed_pattern="s|dbt-core.git.*#subdirectory=core|dbt-core.git@${git_branch}#subdirectory=core|g"
tests_req_sed_pattern="s|dbt-adapters.git.*#subdirectory=dbt-tests-adapter|dbt-adapters.git@${git_branch}#subdirectory=dbt-tests-adapter|g"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "$core_req_sed_pattern" $target_req_file
 sed -i "" "$tests_req_sed_pattern" $target_req_file
else
 sed -i "$core_req_sed_pattern" $target_req_file
 sed -i "$tests_req_sed_pattern" $target_req_file
fi
