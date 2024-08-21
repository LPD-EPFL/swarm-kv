# Unreliable Datagrams

A simple test program that sends/receives messages using UD unicast/multicast.

## Requirements

- [conan](https://conan.io/) package manager

## Build

Preferably use our python build script under the root of the repo and call it with:

```sh
./build.py ud
```

That script detects local dependency changes and re-builds them before building this module.

Alternatively from within this folder you can run:

```sh
conan create . --build=outdated
```

## Network configuration

### Unicast

In order to use unicast, make sure that the sender can reach the receiver:

```sh
ibtracert <SENDER_LID> <RECEIVER_LID>
```

### Multicast

In order to use multicast, make sure that all the receivers are reachable through a multicast group:

```sh
ibswitches #get the LID of the switch
ibroute -M <SWITCH_LID> #get the LIDs of the multicast groups, if none, configure them on the SA
saquery -g #get the GIDs (ipv6 format) of the previously found multicast groups
ibtracert -m <MULTICAST_GROUP_LID> <SENDER_LID> <RECEIVER_LID> #checks that SENDER can indeed reach RECEIVER through MULTICAST_GROUP
```

## Run

### Unicast

Start the executable on the receivers:

```sh
doryud -n <NB_MSG_TO_RECV>
```

Note the output serialized receiver information `<HEX_LID>/<HEX_QPN>/<HEX_PKEY>`.

Run the executable on the sender:

```sh
doryud -s -n <NB_MSG_TO_SEND> [-r <SRLZD_RECV_INFO>]...
```

### Unicast using UdConnectionExchanger

You don't have to copy the serialized receiver informations as they can be
transfered through an exchanger. In order to use it, you must have a memcached
instance reachable and export the `DORY_REGISTRY_IP` environnement variable.

Start the executable on the receivers with the `-x` flag (`--exchanger`):

```sh
doryud -n <NB_MSG_TO_RECV> -x -i <PROC_ID>
```

Run the executable on the sender:

```sh
doryud -s -n <NB_MSG_TO_SEND> -x -p <NUM_PROC> -i <PROC_ID>
```

Note: the sender will wait for every connection in the set {0..NUM_PROC-1}/
{PROC_ID} to be announced (IDs should be contiguous, etc.).

### Multicast

A multicast group serialized information has the format `<GID_IPV6>/<HEX_LID>`.

Start the executable on the receivers:
```sh
doryud -n <NB_MSG_TO_RECV_PER_MC_GROUP> [-m <MC_SRLZD_INFO>]...
```

Note: the receivers will wait for `NB_MSG_TO_RECV_PER_MC_GROUP * |MC_GROUPS|` messages on any group.

Start the executable on the sender:
```sh
doryud -s -n <NB_MSG_TO_SEND_TO_EACH_GROUP> [-m <MC_SRLZD_INFO>]...
```

Note: the sender will send `NB_MSG_TO_SEND_TO_EACH_GROUP` to each multicast group.
