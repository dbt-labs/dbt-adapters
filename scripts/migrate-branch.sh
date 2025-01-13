_USER=$1  # your github user
_BRANCH=$2  # your feature branch
_REPO=$3  # the repo name (the adapter)

# create a remote for the adapter repo (supports forks via _USER)
git remote add adapter https://github.com/$_USER/$_REPO.git
git fetch adapter

# update your feature branch against dbt-adapters@main
git rebase main adapter/$_BRANCH

# create a branch in the dbt-adapters repo for your feature branch from the adapter repo
git checkout -b $_REPO/$_BRANCH  # prefixing <adapter>/ namespaces your feature branch in the new repo
git merge adapter/$_BRANCH
git push origin $_REPO/$_BRANCH

# remove the remote that was created by this process
git remote remove adapter
