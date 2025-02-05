# this script allows you to backport a change from dbt-adapters into a legacy adapter repo

ADAPTER=$1
BRANCH=$2
COMMIT=$3

# add a new remote for the legacy repo
git remote add $ADAPTER https://github.com/dbt-labs/$ADAPTER.git
git fetch

# create a new branch off of the target branch
git switch $ADAPTER/$BRANCH
git switch -c $ADAPTER/backport-$COMMIT-to-$BRANCH

# cherry pick the commit from dbt-adapters into the new branch
git cherry-pick $COMMIT -x

# manually move new files where they need to be
# in particular, new files will not be moved automatically since there is no reference in the target repo

# continue the cherry pick process after resolving conflicts
git cherry-pick --continue -e

# :x!<enter> to accept the message

# push the new branch up to the legacy repo
git push $ADAPTER backport-$COMMIT-to-$BRANCH

# remove the remote that was created during this process
git remote remove $ADAPTER
