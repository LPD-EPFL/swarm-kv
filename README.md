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


## Navigating the code

Unfortunately, the algorithms presented in the paper were heavily factorized vs our implementation, so there's no clearly defined place where abstraction X or Y is implemented, but the rough mapping is:
- In-n-Out and the CAS-based MAX-replacement are located in [unreliable_maxreg.hpp](swarm-kv/src/unreliable_maxreg.hpp).
- [relaxed_maxreg.hpp](swarm-kv/src/relaxed_maxreg.hpp) uses the above and provides the replicated max register abstractions and ensures operations are executed on a majority of servers.
- [oops_data_future.hpp](swarm-kv/src/oops_data_future.hpp) uses the above and provides Safe-Guess' logic.
- The above components implements state machines where the "`step`" is the current state in the respective algorithms and RDMA requests are sent by [unreliable_maxreg.hpp](swarm-kv/src/unreliable_maxreg.hpp) when transitioning between steps.
- Timestamp validators are part of the `RTryNotifyWriter` and `WTryRepare` steps of [oops_data_future.hpp](swarm-kv/src/oops_data_future.hpp).
- [tspointers.hpp](swarm-kv/src/tspointers.hpp) corresponds to metadata buffers that both contain a timestamp and a pointer to the out-of-place data.
- [oops_client.hpp](swarm-kv/src/oops_client.hpp) initiates and manages all instances of [oops_data_future.hpp](swarm-kv/src/oops_data_future.hpp).
- [oops_state.hpp](swarm-kv/src/oops_state.hpp) stores state and configuration shared between the client and all the underlying futures.
- [layout.hpp](swarm-kv/src/layout.hpp) contains the system parametters and allow for easy translations from element indices to RDMA-accessible memory locations in clients and servers.
- [latency.hpp](swarm-kv/src/latency.hpp) allows for efficient percentile latency measurements.
- [main.cpp](swarm-kv/src/main.cpp) handles all the arguments, bootstraps the client or server, and generates and executes the workloads on clients.
- The [race](swarm-kv/src/race) subfolder contains a simplified implementation of RACE, a low-latency RDMA-based index.

All the above are contained in the swarm-kv submodule.

The repository also includes the following submodules:
- fusee: an implementation of the FUSEE KVS
- conn: provides interfaces to easily create and manage reliable connections over RDMA.
- ctrl: provides interfaces to manage RDMA devices and control blocks.
- memory: provides datastructures used by ctrl
- extern: provides the ibverbs and memcached external librairies
- memstore: provides an interface to access memcached. This module is used by conn to simplify the coordination of clients and servers for the configuration of RDMA connections.
- shared: provides various tools used by multiple submodules
- third-party: provides third-party tools (used by conn)
- special: provides cmd-lines tools

The dependencies are described in [targets.yaml](targets.yaml)
