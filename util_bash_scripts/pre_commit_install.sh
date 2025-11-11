#!/bin/bash

echo "***RUNNING PIP INSTALL PRE-COMMIT***"
pip3 install --constraint ./requirements-constraints.txt pre-commit

echo "***PIP CACHE CLEAN***"
pip3 cache purge

echo "***RUNNING PRE-COMMIT INSTALL***"
pre-commit install
