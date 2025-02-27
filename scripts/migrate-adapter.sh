ADAPTER=$1
SOURCE_BRANCH=$2
TARGET_BRANCH=$3

# create a remote for the legacy adapter repo and fetch the latest commits
git remote remove adapter || true
git remote add adapter https://github.com/dbt-labs/$ADAPTER.git
git fetch adapter

# merge the updated branch from the legacy repo into the dbt-adapters repo
git checkout $TARGET_BRANCH
git merge adapter/$SOURCE_BRANCH --allow-unrelated-histories

# remove the remote that was created by this process
git remote remove adapter || true

# manually clean up duplication or unwanted files
