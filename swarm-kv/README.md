# SWARM-KV

A fault-tolerant RDMA-based disaggregated key-value store with 1-RTT UPDATEs and GETs thanks to the SWARM replication protocol

## Requirements

- [conan](https://conan.io/) package manager

## Build

Preferably use our python build script under the root of the repo and call it with:

```sh
./build.py swarm-kv
```

That script detects local dependency changes and re-builds them before building this module.

Alternatively from within this folder you can run:

```sh
conan create . --build=outdated
```

