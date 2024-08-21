#!/bin/bash

set -e

CONANDEP=$1

if conan export . 2>&1 | grep -q "The stored package has not changed" ; then 
    exit 0
fi

mkdir -p "$(dirname $CONANDEP)" && touch $CONANDEP
