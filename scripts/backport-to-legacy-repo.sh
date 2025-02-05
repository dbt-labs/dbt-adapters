# this script allows you to backport a change from dbt-adapters into a legacy adapter repo
# it should be run from the adapter repo, but because it's the same script for all adapters, it's saved here
# TODO: update this script to run from here

$COMMIT

git remote add monorepo https://github.com/dbt-labs/dbt-adapters.git
git fetch

git cherry-pick $COMMIT -x

# manually move new files where they need to be
# in particular, new files will not be moved automatically since there is no reference in the target repo

git cherry-pick --continue -e

git remote remove monorepo
