#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Detect if a command exits. Additionally, return the command if it exits
function tidy_command_exists() {
    local cmd=$1
    if ! command -v "$cmd" &> /dev/null; then
        exit 1
    else
        echo "$cmd"
        exit 0
    fi
}

# Stop checking once a working command is found
while : ; do
    # Tidy command for ubuntu
    tidy_command=$(tidy_command_exists run-clang-tidy-10) && break

    # Tidy command for archlinux
    tidy_command=$(tidy_command_exists /usr/share/clang/run-clang-tidy.py) && break
    
    # Guard (do not remove)
    echo "clang-tidy command could not be found"
    exit 1
done

modified=0
if [ $# -eq 0 ]
then
    cd "$DIR" || return
    for package in `ls "$DIR"/.clang-tidy-builds`; do
        # Check if the project is compiled with clang
        compiler=$(cat "$DIR"/.clang-tidy-builds/"$package"/compiler.txt)
        if [[ $compiler != clang* ]]
        then
            echo "Cannot tidy $package. Tidyer requires compilation with Clang"
            exit 2
        fi

        echo "Working on $package"
        "$tidy_command" -quiet -fix -j $(nproc) -header-filter='.*\.hpp' -format -p="$DIR"/.clang-tidy-builds/"$package" -extra-arg="-DCLANG_TIDIER"
        tidy_ret=$?
        if [ $tidy_ret -ne 0 ]
        then
            echo "exit code $tidy_ret when tidying $package"
            modified=1
        fi
        echo "Exit code: $tidy_ret"
	echo
    done
else
    echo "Tidying up directories: ${*:1}"
    for package in "$@"; do
        # Check if the project is compiled with clang
        compiler=$(cat "$DIR"/.clang-tidy-builds/"$package"/compiler.txt)
        if [[ $compiler != clang* ]]
        then
            echo "Cannot tidy $package. Tidyer requires compilation with Clang"
            exit 2
        fi

        echo "Working on $package"
        "$tidy_command" -quiet -fix -j $(nproc) -header-filter='.*\.hpp' -format -p="$DIR"/.clang-tidy-builds/"$package" -extra-arg="-DCLANG_TIDIER"
        tidy_ret=$?
        if [ $tidy_ret -ne 0 ]
        then
            echo "exit code $tidy_ret when tidying $package"
            modified=1
        fi
        echo "Exit code: $tidy_ret"
	echo
    done
fi
exit $modified
