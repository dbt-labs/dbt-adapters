# 1. Run this script to migrate your feature branch from an adapter repo/fork to the dbt-adapters repo/fork, e.g.:
#    $> source ./scripts/migrate-branch.sh dbt-labs dbt-postgres my-cool-feature-branch
# 2. Resolve any resulting merge conflicts.
# 3. Push the resulting branch back up to the dbt-adapters repo/fork, e.g.:
#    $> git push origin dbt-postgres/my-cool-feature-branch

user=$1    # your github user (e.g. dbt-labs)
repo=$2    # the repo name (e.g. dbt-postgres)
branch=$3  # your feature branch (e.g. my-cool-feature-branch)

# create a remote for the adapter repo (supports forks)
git remote remove adapter || true
git remote add adapter https://github.com/$user/$repo.git
git fetch adapter

# create a branch in the dbt-adapters repo for your feature branch from the adapter repo
git checkout -b $repo/$branch  # prefixing <adapter>/ namespaces your feature branch in the new repo

# update your feature branch against dbt-adapters@main
git merge adapter/$branch

# remove the remote that was created by this process
git remote remove adapter
