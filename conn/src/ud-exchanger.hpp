#pragma once

#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <thread>
#include <utility>

#include <vector>

#include <dory/ctrl/block.hpp>
#include <dory/memstore/store.hpp>
#include <dory/shared/logger.hpp>

#include "ud.hpp"

namespace dory::conn {
// Todo: generic over container + unsigned requirement?
template <typename ProcId>
class UdConnectionExchanger {
 private:
  static auto constexpr RetryTime = std::chrono::milliseconds(20);

 public:
  UdConnectionExchanger(memstore::MemoryStore& store, ctrl::ControlBlock& cb,
                        std::string pd_name,
                        std::shared_ptr<UnreliableDatagram> shared_ud)
      : store{store},
        cb{cb},
        pd_name{std::move(pd_name)},
        shared_ud{std::move(shared_ud)},
        LOGGER_INIT(logger, "UD-CE") {}

  void announce(ProcId my_id, std::string const& prefix) {
    std::stringstream name;
    name << prefix << "-" << my_id << "-ud";
    store.set(name.str(), shared_ud->info().serialize());
    LOGGER_INFO(logger, "Publishing ud-qp {}", name.str());
  }

  void connect(ProcId proc_id, std::string const& prefix) {
    std::stringstream name;
    name << prefix << "-" << proc_id << "-ud";

    std::string serialized_ud;
    if (!store.get(name.str(), serialized_ud)) {
      LOGGER_DEBUG(logger, "Could not retrieve key {}", name.str());

      throw std::runtime_error("Cannot connect to remote qp " + name.str());
    }

    udcs.emplace(proc_id, UnreliableDatagramConnection{cb, pd_name, shared_ud,
                                                       serialized_ud});
    LOGGER_INFO(logger, "Connected ud with {}", name.str());
  }

  void connectAll(std::vector<ProcId> remote_ids, std::string const& prefix) {
    for (auto pid : remote_ids) {
      connect(pid, prefix);
    }
  }

  void announceReady(ProcId my_id, std::string const& prefix,
                     std::string const& reason) {
    std::stringstream name;
    name << prefix << "-" << my_id << "-ud-ready(" << reason << ")";
    store.set(name.str(), "ready(" + reason + ")");
  }

  void waitReady(ProcId proc_id, std::string const& prefix,
                 std::string const& reason) {
    auto packed_reason = "ready(" + reason + ")";
    std::stringstream name;
    name << prefix << "-" << proc_id << "-ud-" << packed_reason;

    auto key = name.str();
    std::string value;

    while (!store.get(key, value)) {
      std::this_thread::sleep_for(RetryTime);
    }

    if (value != packed_reason) {
      throw std::runtime_error("Ready announcement of message `" + key +
                               "` does not contain the value `" +
                               packed_reason + "`");
    }
  }

  void waitReadyAll(std::vector<ProcId> const& remote_ids,
                    std::string const& prefix, std::string const& reason) {
    for (auto pid : remote_ids) {
      waitReady(pid, prefix, reason);
    }
  }

  std::map<ProcId, UnreliableDatagramConnection>& connections() { return udcs; }

 private:
  memstore::MemoryStore& store;
  ctrl::ControlBlock& cb;
  std::string pd_name;
  std::shared_ptr<UnreliableDatagram> shared_ud;
  std::map<ProcId, UnreliableDatagramConnection> udcs;
  LOGGER_DECL(logger);
};
}  // namespace dory::conn
