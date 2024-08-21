#!/bin/bash

set -e

CONANDEP=$1

# Create and build the package in the conan cache directory
# Consider the current package outdated, otherwise conan will not build
# the current package if a dependency is rebuilt. Instead will keep using
# the old package.
conan create . --build=outdated --build="$(./conanfile.py --name-only)" --test-folder=None

# Run the deploy method of the conanfile to copy all the build artifacts
conan install $(./conanfile.py --name-only)@ --install-folder=build

mkdir -p "$(dirname $CONANDEP)" && touch $CONANDEP
