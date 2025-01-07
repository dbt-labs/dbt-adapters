repo=$1
source_branch=$2
target_branch=$3

# create a remote for the legacy adapter repo and fetch the latest commits
git remote remove old || true
git remote add old https://github.com/dbt-labs/$repo.git
git fetch old

# merge the updated branch from the legacy repo into the dbt-adapters repo
git checkout -b $target_branch
git merge old/$source_branch --allow-unrelated-histories

# remove the remote that was created by this process
git remote remove old || true

# manually clean up duplication or unwanted files
