#!/bin/bash

echo "***RUNNING GIT GC --PRUNE=ALL***"
git gc --prune=all

echo "***REMOVING CACHE DIRECTORIES***"
rm -rf .pytest_cache
rm -rf .mypy_cache

echo "***ADDING CHANGES TO GIT STAGE***"
git add .

echo "***COMMITTING CHANGES TO GIT STAGE***"
read -r -p "Enter your commit message: " message
git commit --message "$message"
