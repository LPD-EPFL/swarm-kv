#!/bin/bash

set -e

DORY_TIDYDIR_TARGET="$1"

rm -rf "$DORY_TIDYDIR_TARGET"

conan install . --build=outdated \
    --install-folder="$DORY_TIDYDIR_TARGET"

# Do not export earlier, as it causes dependencies (built in ~/.conan) to be built with the export
export CLANG_GEN_TIDY_INFO=1

conan build . --configure \
    --build-folder="$DORY_TIDYDIR_TARGET"
echo "$CC" > "$DORY_TIDYDIR_TARGET"/compiler.txt
