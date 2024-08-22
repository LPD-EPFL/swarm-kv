# SWARM-KV

SWARM is a wait-free replication protocol for shared data in disaggregated memory that provides 1-RTT reads and writes.

SWARM-KV is an RDMA-based disaggregated key-value store that leverages SWARM to offer 1-RTT GETs and UPDATEs.

## Requirements

- [conan](https://conan.io/) package manager
    ```sh
    pip3 install --user "conan>=1.47.0,<2.0"
    ```

    make sure to set the default ABI to C++11 with:

    ```sh
    conan profile new default --detect  # Generates default profile detecting GCC and sets old ABI
    conan profile update settings.compiler.libcxx=libstdc++11 default  # Sets libcxx to C++11 ABI
    ```

- cmake v3.9.x
- clang-format >= v6.0.0

## Build

Run from within the root:

```sh
./build.py swarm-kv
```

This will create all conan packages required by swarm-kv, and its executables.

__Note:__ If `gcc` is available, it is used as the default compiler. In a system with `clang` only, then `clang` becomes the default compiler. In any case, you can check the available compilers/compiler versions by calling `./build.py --help`.

## Docker

You can manually build the [Dockerfile](https://github.com/LPD-EPFL/swarm-kv/blob/master/Dockerfile) under the root of this repo.

```sh
docker build -t dory .
```
---


## Usage

Refer to [SWARM-KV's artifact repository](https://github.com/LPD-EPFL/swarm-artifacts) for detailed instructions on how to run the experiments.
