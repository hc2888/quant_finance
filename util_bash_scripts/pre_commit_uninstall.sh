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
