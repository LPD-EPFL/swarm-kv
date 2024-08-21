#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$DIR"

nonverbose=$1

GCC_SUPPORTED_VERSIONS=( 7 8 9 10 11 12 )
CLANG_SUPPORTED_VERSIONS=( 6.0 7 8 9 10 11 12 )

if which gcc > /dev/null 2>&1; then
    if ! which g++ > /dev/null 2>&1; then
       [[ -z "$nonverbose" ]] && >&2 echo "[Warning] gcc was found but not g++"
    else
        gcc_ver=`gcc -dM -E -x c /dev/null | grep -E -o '__GNUC__ [[:digit:]]+' | awk '{ print $2 }'`
        gpp_ver=`g++ -dM -E -x c /dev/null | grep -E -o '__GNUC__ [[:digit:]]+' | awk '{ print $2 }'`

        if [ "$gcc_ver" -eq "$gpp_ver" ]; then
            if [ "$1" != "--nodefaults" ]; then
                echo $gcc_ver > gcc-default && echo gcc
            fi
        fi
    fi
fi

for version in "${GCC_SUPPORTED_VERSIONS[@]}"; do
    rm -rf gcc-$version

    if which gcc-$version > /dev/null 2>&1; then
        if ! which g++-$version > /dev/null 2>&1; then
            [[ -z "$nonverbose" ]] && >&2 echo "[Warning] gcc-$version was found but not g++-$version"
            continue
        fi

        echo gcc-$version
        mkdir -p gcc-$version
        cd gcc-$version
        ln -s `which gcc-$version` gcc
        ln -s `which g++-$version` g++
        cd ..
    fi
done

if which clang > /dev/null 2>&1; then
    if ! which clang++ > /dev/null 2>&1; then
        [[ -z "$nonverbose" ]] && >&2 echo "[Warning] clang was found but not clang++"
    else
        clang_ver=`clang -dM -E -x c /dev/null | grep -E -o '__clang_major__ [[:digit:]]+' | awk '{ print $2 }'`
        clangpp_ver=`clang++ -dM -E -x c /dev/null | grep -E -o '__clang_major__ [[:digit:]]+' | awk '{ print $2 }'`

        if [ "$clang_ver" -eq "$clangpp_ver" ]; then
            if [ "$1" != "--nodefaults" ]; then
                echo $clang_ver > clang-default && echo clang
            fi
        fi
    fi
fi

for version in "${CLANG_SUPPORTED_VERSIONS[@]}"; do
    # Remove `.0`
    stripped_version="${version%.*}"

    rm -rf clang-$stripped_version

    if which clang-$version > /dev/null 2>&1; then
        if ! which clang++-$version > /dev/null 2>&1; then
            [[ -z "$nonverbose" ]] && >&2 echo "[Warning] clang-$version was found but not clang++-$version"
            continue
        fi

        echo clang-$stripped_version
        mkdir -p clang-$stripped_version
        cd clang-$stripped_version
        ln -s `which clang-$version` clang
        ln -s `which clang++-$version` clang++
        cd ..
    fi
done
