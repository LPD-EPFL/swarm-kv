#!/bin/bash

set -e

CONANDEP=$1

# Create and build the package in the conan cache directory
# Do not export the package, as other packages trying to build
# may be using it in parallel
conan create --not-export . --build=outdated --test-folder=test

# mkdir -p "$(dirname $CONANDEP)" && touch $CONANDEP
