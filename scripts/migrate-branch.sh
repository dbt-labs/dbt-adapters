# 1. Run this script to migrate your feature branch from an adapter repo/fork to the dbt-adapters repo/fork, e.g.:
#    > source ./scripts/migrate-branch.sh dbt-labs dbt-postgres my-cool-feature-branch
# 2. Resolve any conflicts resulting from the rebase.

USER=$1     # your github user (e.g. dbt-labs)
ADAPTER=$2  # the repo name (e.g. dbt-postgres)
BRANCH=$3   # your feature branch (e.g. my-cool-feature-branch)

# create a remote for the adapter repo (supports forks)
git remote add adapter https://github.com/$USER/$ADAPTER.git || true  # this may already exist from a previous run

# update your feature branch against dbt-adapters@main and potentially resolve conflicts
git fetch adapter
git rebase main adapter/$BRANCH

# create a branch in the dbt-adapters repo for your feature branch from the adapter repo
git checkout -b $ADAPTER/$BRANCH  # prefixing <adapter>/ namespaces your feature branch in the new repo
git merge adapter/$BRANCH

# manually resolve any conflicts and move new files where they need to be
# in particular, new files will not be moved automatically since there is no reference in the target repo

# push the new branch up to the legacy repo
git push adapter $BRANCH

# remove the remote that was created during this process
git remote remove adapter || true
