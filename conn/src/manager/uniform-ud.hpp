#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include <dory/ctrl/block.hpp>

#include "../ud.hpp"

namespace dory::conn::manager {

/**
 * Connection Manager where all connections have the same characteristics
 */
template <typename ProcIdType>
class UniformUdConnectionManager {
 public:
  UniformUdConnectionManager(ctrl::ControlBlock &cb,
                             std::shared_ptr<UnreliableDatagram> shared_ud)
      : cb{cb}, shared_ud{std::move(shared_ud)} {}

  void usePd(std::string const &pd) { this->pd = pd; }

  UnreliableDatagramInfo remoteInfo() const { return shared_ud->info(); }

  UnreliableDatagramConnection &newConnection(
      ProcIdType proc_id, std::string const &serialized_ud) {
    if (rcs.find(proc_id) != rcs.end()) {
      throw std::runtime_error("Connection for process " +
                               std::to_string(proc_id) + " already exists!");
    }

    rcs.insert({proc_id, UnreliableDatagramConnection(cb, pd, shared_ud,
                                                      serialized_ud)});
    auto &rc = rcs.find(proc_id)->second;

    return rc;
  }

  void removeConnection(ProcIdType proc_id) { rcs.erase(proc_id); }

 private:
  std::string pd;

  ctrl::ControlBlock &cb;
  std::shared_ptr<UnreliableDatagram> shared_ud;

  std::unordered_map<ProcIdType, UnreliableDatagramConnection> rcs;
};
}  // namespace dory::conn::manager
