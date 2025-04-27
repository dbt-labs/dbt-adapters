RUN_ID=$1   # the run to cancel (https://github.com/dbt-labs/dbt-adapters/actions/runs/<THIS-VALUE>)

gh api \
    --method POST \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    /repos/dbt-labs/dbt-adapters/actions/runs/$RUN_ID/force-cancel
