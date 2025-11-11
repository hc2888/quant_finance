#!/bin/bash

echo "***RUNNING GIT GC --PRUNE=ALL***"
git gc --prune=all

echo "***RUNNING PIP UNINSTALL --YES PRE-COMMIT***"
pip3 uninstall --yes pre-commit

echo "***PIP CACHE CLEAN***"
pip3 cache purge

echo "***REMOVING PRE-COMMIT HOOKS***"
find .git -type f -name "pre-commit" -exec rm {} \;

echo "***REMOVING CACHE DIRECTORIES***"
rm -rf .pytest_cache
rm -rf .mypy_cache

echo "***COMPLETELY RESET GIT REPO***"
git reset --hard HEAD
git checkout --orphan temp_branch
git add -A

# Format the date in YYYY-MM-DD format and time in 12-hour format with AM/PM
mydate=$(date +"%Y-%m-%d")
mytime=$(date +"%I:%M:%S %p")
timestamp="$mydate $mytime"
git commit -am "$timestamp"

git branch -D main
git branch -m main
git push -f origin main
git clean -fd

echo "***RUNNING PIP INSTALL PRE-COMMIT***"
pip3 install --constraint ./requirements-constraints.txt pre-commit

echo "***PIP CACHE CLEAN AGAIN***"
pip3 cache purge

echo "***RUNNING PRE-COMMIT INSTALL***"
pre-commit install

echo "***TIMESTAMP: $timestamp***"
