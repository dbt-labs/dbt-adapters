# 1. Run this script to backport a change from the monorepo to a legacy repo, e.g.:
#    > source ./scripts/backport-to-legacy-repo.sh dbt-postgres 1.9.latest abc1234
# 2. Resolve any conflicts resulting from the cherry pick.
#    In particular, git will not know to move a new file out of the package directory and into the root.

ADAPTER=$1  # the repo name (e.g. dbt-postgres)
BRANCH=$2   # the target branch (e.g. 1.9.latest)
COMMIT=$3   # the commit SHA to backport

# add a new remote for the legacy repo
git remote add adapter https://github.com/dbt-labs/$ADAPTER.git
git fetch

# create a new branch off of the target branch
git switch adapter/$BRANCH
git switch -c adapter/backport-$COMMIT-to-$BRANCH

# cherry pick the commit from dbt-adapters into the new branch
git cherry-pick $COMMIT -x

# manually resolve any conflicts and move new files where they need to be
# in particular, new files will not be moved automatically since there is no reference in the target repo

# continue the cherry pick process after resolving conflicts
git cherry-pick --continue -e

# :x!<enter> to accept the message

# push the new branch up to the legacy repo
git push adapter backport-$COMMIT-to-$BRANCH

# remove the remote that was created during this process
git remote remove adapter || true
