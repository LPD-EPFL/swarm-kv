#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

function find_source_files() {
    find . \
        \( -not -path "*/build/*" \
           -not -path "./.clang-tidy-builds/*" \
           -not -path "./.deps/*" \
           -not -path "./.git/*" \
           -not -path "./.jenkins/*/dory/*" \
           -not -path "./third-party/*/internal/*" \
            \( -name "*.cpp" -o \
               -name "*.hpp" -o \
               -name "*.c" -o \
               -name "*.h" \
            \) \
        \) -exec "$@"
}

function find_cmake_files() {
    find . \
        \( -not -path "*/build/*" \
           -not -path "./.clang-tidy-builds/*" \
           -not -path "./.deps/*" \
           -not -path "./.git/*" \
           -not -path "./.jenkins/*/dory/*" \
            \( -name "CMakeLists.txt" \) \
        \) -exec "$@"
}

function find_python_files() {
    find . \
        \( -not -path "*/build/*" \
           -not -path "./.clang-tidy-builds/*" \
           -not -path "./.deps/*" \
           -not -path "./.git/*" \
           -not -path "./.jenkins/*/dory/*" \
            \( -name "*.py" \) \
        \) -exec "$@"
}

function compute_md5sum() {
    local output=`$1 cat {} + | md5sum`
    echo $output | awk '{print $1}'
}

# If `-v` is provided as argument, the formatter scans all files
# end returns the exit code `1` if any change was found. If `-v` is
# not provided, then the exit code will be `0`, unless one of the
# formatters fails to format some files.
verbose=false
if [ $# -eq 1 ]; then
    if [[ "$1" == "-v" ]]; then
        verbose=true
        shift
    fi
fi

if [ $# -eq 0 ]
then
    cd "$DIR" || exit 1

    changed=false

    echo "No arguments supplied, running on the entire source"
    
    # Execute `clang-format` once for all files (denoted by `+``)
    # Replace with `\;` to execute once for every file.
    
    source_md5_before=$(compute_md5sum find_source_files)
    find_source_files clang-format-10 -i -style=file {} +
    find_source_files awk -i inplace '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' {} +
    source_md5_after=$(compute_md5sum find_source_files)
    
    [[ "$source_md5_before" != "$source_md5_after" ]] && changed=true

    cmake_md5_before=$(compute_md5sum find_cmake_files)
    find_cmake_files cmake-format -i {} +
    find_cmake_files awk -i inplace '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' {} +
    cmake_md5_after=$(compute_md5sum find_cmake_files)

    [[ "$cmake_md5_before" != "$cmake_md5_after" ]] && changed=true

    python_md5_before=$(compute_md5sum find_python_files)
    find_python_files black -q -t py38 {} +
    find_python_files awk -i inplace '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' {} +
    python_md5_after=$(compute_md5sum find_python_files)

    [[ "$python_md5_before" != "$python_md5_after" ]] && changed=true

    if $changed; then
        echo "Formatting made some changes"

        if $verbose; then
            git diff
            exit 1
        fi
    fi
else
    echo "Formatting files: ${*:1}"

    for path_name in ${*:1}; do
        filename=$(basename -- "$path_name")
        extension=$([[ "$filename" = *.* ]] && echo ".${filename##*.}" || echo '')

        skipped=false
        case "$extension" in
            .c|.cpp|.h|.hpp)
                clang-format-10 -i -assume-filename="$DIR"/.clang-format -style=file "$path_name" ;;
            .py)
                black -q -t py38 "$path_name" ;;
            .txt)
                if [[ $filename == "CMakeLists.txt" ]]; then
                    cmake-format -i "$path_name"
                else
                    skipped=true
                fi
                ;;
            *)
                echo "Skipping $path_name"
                skipped=true
                ;;
        esac

        if ! $skipped ; then
            awk -i inplace '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' "$path_name"
        fi
    done
fi
