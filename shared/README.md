# Shared

## Requirements

- [conan](https://conan.io/) package manager

## Install

Run from within this folder:

```sh
conan create .
```

Which will create this package in the local conan cache.

## Usage

Inside a `conanfile.txt` specify:

```toml
[requires]
dory-shared/0.0.1
```

and use in source files as follows:

```cpp
#include <dory/shared/bench.hpp>
#include <dory/shared/branching.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/pointer-wrapper.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/unused-suppressor.hpp>

// For the units:
using namespace dory::units;
```
