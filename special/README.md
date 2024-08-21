# Handling of compiler and/or Linux special features

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
dory-special/0.0.1
```

Use the lib in the source files as follows:

```cpp
#include <dory/special/init-array.hpp>
#include <dory/special/proc-name.hpp>
#include <dory/special/heartbeat.hpp>
```
