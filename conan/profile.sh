#!/bin/bash

# This file is supposed to be sourced from the build scripts

parse_profile () {
    local DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
    cd $DIR

    help() {
        echo "Usage : $0 <compiler> <build_type>"
        echo ""
        echo "<compiler> can be one of the following:"
        echo "$($DIR/parse-compilers.py --show-compilers)"
        echo ""
        echo "<build_type> can be one of the following:"
        echo "$($DIR/parse-compilers.py --show-builds)"
    }

    if [ $# -lt 2 ]
    then
        help
        exit 1
    fi
    
    local exports; exports=$($DIR/parse-compilers.py -c "$1" -b "$2")
    if [ $? -ne 0 ]; then
        help
        exit 1
    else
        eval $exports
    fi
}

parse_profile "$@"
