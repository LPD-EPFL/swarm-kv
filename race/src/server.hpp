#pragma once

#include <vector>

#include <dory/shared/logger.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/extern/ibverbs.hpp>
#include <dory/shared/types.hpp>

#include "types.hpp"

namespace dory::race {

class Server {
  LOGGER_DECL_INIT(logger, "Server");
  size_t bucket_bits;

  static size_t constexpr index_size(size_t const bucket_bits) {
    auto const nb_buckets = (1 << (bucket_bits - 1)) * 3; 
    return nb_buckets * sizeof(Bucket);
  }

  // RDMA
  ctrl::OpenDevice device;
  Delayed<ctrl::ControlBlock> cb;
  Delayed<conn::ReliableConnection> rc;
public:
  Server(ProcId const id, std::vector<ProcId> const& client_ids, size_t const bucket_bits, std::string const& name="default")
  : bucket_bits{bucket_bits},
    device{std::move(ctrl::Devices().list()[0])} {
  
    size_t const allocated_size = index_size(bucket_bits);

    ctrl::ResolvedPort port{device};
    if (!port.bindTo(0)) throw std::runtime_error("Couldn't bind the device.");
    cb.emplace(port);
    cb->registerPd("pd");
    cb->allocateBuffer("buf", allocated_size, 64);
    cb->registerMr(
        "mr", "pd", "buf",
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE | ctrl::ControlBlock::REMOTE_ATOMIC);
    cb->registerCq("cq");

    // 4. We establish reliable connections.
    auto& store = memstore::MemoryStore::getInstance();

    dory::conn::RcConnectionExchanger<ProcId> ce(id, client_ids, *cb);
    auto const topic = fmt::format("{}-race-qp", name);
    ce.configureAll("pd", "mr", "cq", "cq");
    ce.announceAll(store, topic);
    ce.announceReady(store, topic, "prepared");
    logger->info("Connecting...");
    ce.waitReadyAll(store, topic, "prepared");
    ce.connectAll(
      store, topic,
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
      ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE | ctrl::ControlBlock::REMOTE_ATOMIC);
    logger->info("Connected");
    logger->info("RDMA buffer: {} to {}", cb->mr("mr").addr, cb->mr("mr").addr + cb->mr("mr").size);
    ce.announceReady(store, topic, "connected");
    ce.waitReadyAll(store, topic, "connected");
  }
};

}