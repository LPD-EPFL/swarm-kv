# Fusee

An RDMA KVStore tailored for spot VMs.

## Requirements

- [conan](https://conan.io/) package manager

## Build

Preferably use our python build script under the root of the repo and call it with:

```sh
./build.py fusee
```

That script detects local dependency changes and re-builds them before building this module.

Alternatively from within this folder you can run:

```sh
conan create . --build=outdated
```

## Network configuration

TODO

## Run

TODO
