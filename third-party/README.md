# Third party

This package contains third party header libraries used among the dory source.

## Requirements

- [conan](https://conan.io/) package manager

## Install

Run from within this folder:

```sh
conan create .
```

## Usage

Inside a `conanfile.txt` specify:

```toml
[requires]
dory-third-party/0.0.1
```

and use in source files as follows:

```cpp
#include <dory/third-party/sync/spsc.hpp>
```
