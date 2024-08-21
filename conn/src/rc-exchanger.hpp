#pragma once

#include <chrono>
#include <cstddef>
#include <future>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#include <dory/ctrl/block.hpp>
#include <dory/memstore/store.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/types.hpp>

#include "rc.hpp"

namespace dory::conn {

namespace internal {
enum NoRoles {};
}

// Todo: generic over container + unsigned requirement?
template <typename ProcId, typename Role = internal::NoRoles>
class RcConnectionExchanger {
 private:
  static auto constexpr RetryTime = std::chrono::milliseconds(20);

 public:
  RcConnectionExchanger(ProcId my_id, std::vector<ProcId> remote_ids,
                        ctrl::ControlBlock& cb)
      : my_id{my_id},
        remote_ids{remote_ids},
        cb{cb},
        LOGGER_INIT(logger, "CE") {
    checkIds();
  }

  RcConnectionExchanger(ProcId my_id, std::vector<ProcId> remote_ids,
                        ctrl::ControlBlock& cb, Role my_role, Role remote_roles)
      : RcConnectionExchanger(my_id, remote_ids, cb) {
    my_role_str = ":" + std::to_string(static_cast<int>(my_role));
    remote_roles_str = ":" + std::to_string(static_cast<int>(remote_roles));
  }

  void configure(ProcId proc_id, std::string const& pd, std::string const& mr,
                 std::string send_cq_name, std::string recv_cq_name) {
    if (rcs.find(proc_id) != rcs.end()) {
      throw std::runtime_error("proc id " + std::to_string(+proc_id) +
                               " has already been configured.");
    }
    rcs.try_emplace(proc_id, cb);

    auto& rc = rcs.find(proc_id)->second;

    rc.bindToPd(pd);
    rc.bindToMr(mr);
    rc.associateWithCq(send_cq_name, recv_cq_name);
  }

  void configureAll(std::string const& pd, std::string const& mr,
                    std::string const& send_cq_name,
                    std::string const& recv_cq_name) {
    for (auto const& id : remote_ids) {
      configure(id, pd, mr, send_cq_name, recv_cq_name);
    }
  }

  void announce(ProcId proc_id, memstore::MemoryStore& store,
                std::string const& prefix) {
    auto const rcit = rcs.find(proc_id);
    if (rcit == rcs.end()) {
      throw std::runtime_error("proc id " + std::to_string(+proc_id) +
                               " hasn't been configured.");
    }
    auto& rc = rcit->second;

    std::stringstream name;
    name << prefix << "-" << my_id << my_role_str << "-for-" << proc_id
         << remote_roles_str;
    auto info_for_remote_party = rc.remoteInfo();
    store.set(name.str(), info_for_remote_party.serialize());
    LOGGER_INFO(logger, "Publishing qp {}", name.str());
  }

  void announceAll(memstore::MemoryStore& store, std::string const& prefix) {
    for (auto pid : remote_ids) {
      announce(pid, store, prefix);
    }
  }

  void unannounce(ProcId proc_id, memstore::MemoryStore& store,
                std::string const& prefix) {
    auto const rcit = rcs.find(proc_id);
    if (rcit == rcs.end()) {
      throw std::runtime_error("proc id " + std::to_string(+proc_id) +
                               " hasn't been configured.");
    }
    auto& rc = rcit->second;

    std::stringstream name;
    name << prefix << "-" << my_id << my_role_str << "-for-" << proc_id
         << remote_roles_str;
    auto info_for_remote_party = rc.remoteInfo();
    store.remove(name.str());
    LOGGER_INFO(logger, "Unpublishing qp {}", name.str());
  }

  void unannounceAll(memstore::MemoryStore& store, std::string const& prefix) {
    for (auto pid : remote_ids) {
      unannounce(pid, store, prefix);
    }
  }

  void connect(ProcId proc_id, memstore::MemoryStore& store,
               std::string const& prefix,
               ctrl::ControlBlock::MemoryRights rights =
                   ctrl::ControlBlock::LOCAL_READ) {
    auto const rcit = rcs.find(proc_id);
    if (rcit == rcs.end()) {
      throw std::runtime_error("proc id " + std::to_string(+proc_id) +
                               " hasn't been configured.");
    }
    auto& rc = rcit->second;

    std::stringstream name;
    name << prefix << "-" << proc_id << remote_roles_str << "-for-" << my_id
         << my_role_str;

    std::string ret_val;
    if (!store.get(name.str(), ret_val)) {
      LOGGER_DEBUG(logger, "Could not retrieve key {}", name.str());

      throw std::runtime_error("Cannot connect to remote qp " + name.str());
    }

    auto remote_rc = RemoteConnection::fromStr(ret_val);

    rc.init(rights);
    rc.connect(remote_rc, proc_id);
    LOGGER_INFO(logger, "Connected to qp {} with rights {}", name.str(),
                rights);
  }

  void connectAll(memstore::MemoryStore& store, std::string const& prefix,
                  ctrl::ControlBlock::MemoryRights rights =
                      ctrl::ControlBlock::LOCAL_READ) {
    for (auto pid : remote_ids) {
      connect(pid, store, prefix, rights);
    }
  }

  void announceReady(memstore::MemoryStore& store, std::string const& prefix,
                     std::string const& reason) {
    std::stringstream name;
    name << prefix << "-" << my_id << my_role_str << "-ready(" << reason << ")";
    store.set(name.str(), "ready(" + reason + ")");
  }

  void unannounceReady(memstore::MemoryStore& store, std::string const& prefix,
                     std::string const& reason) {
    std::stringstream name;
    name << prefix << "-" << my_id << my_role_str << "-ready(" << reason << ")";
    store.remove(name.str());
  }

  void waitReady(ProcId proc_id, memstore::MemoryStore& store,
                 std::string const& prefix, std::string const& reason) {
    auto packed_reason = "ready(" + reason + ")";
    std::stringstream name;
    name << prefix << "-" << proc_id << remote_roles_str << "-"
         << packed_reason;

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

  void waitReadyAll(memstore::MemoryStore& store, std::string const& prefix,
                    std::string const& reason) {
    for (auto pid : remote_ids) {
      waitReady(pid, store, prefix, reason);
    }
  }

  std::map<ProcId, ReliableConnection>& connections() {
    if (connections_moved) {
      throw std::runtime_error(
          "Connections have been moved out of the exchanger");
    }

    return rcs;
  }

  ReliableConnection extract(ProcId const proc_id) {
    auto find_it = rcs.find(proc_id);
    if (find_it == rcs.end()) {
      throw std::runtime_error("Cannot extract connection for process " +
                               std::to_string(+proc_id) + ".");
    }

    connections_moved = true;

    ReliableConnection rc{std::move(find_it->second)};
    rcs.erase(find_it);
    return rc;
  }

  void addLoopback(std::string const& pd, std::string const& mr,
                   std::string const& send_cq_name,
                   std::string const& recv_cq_name) {
    loopback_.emplace(cb);
    loopback_->bindToPd(pd);
    loopback_->bindToMr(mr);
    loopback_->associateWithCq(send_cq_name, recv_cq_name);

    LOGGER_INFO(logger, "Loopback connection was added");
  }

  void connectLoopback(ctrl::ControlBlock::MemoryRights rights) {
    auto info_for_remote_party = loopback_->remoteInfo();
    loopback_->init(rights);
    loopback_->connect(info_for_remote_party, my_id);

    LOGGER_INFO(logger, "Loopback connection was established");
  }

  ReliableConnection& loopback() { return *loopback_; }

 private:
  void checkIds() const {
    if (remote_ids.empty()) {
      throw std::runtime_error("No remote Ids exist!");
    }

    auto const min_remote = std::min(remote_ids.begin(), remote_ids.end());
    auto const min = std::min(*min_remote, my_id);

    if (min < 1) {
      throw std::runtime_error("Ids should be positive!");
    }
  }

  ProcId my_id;
  std::vector<ProcId> remote_ids;
  ctrl::ControlBlock& cb;
  std::string my_role_str;
  std::string remote_roles_str;
  std::map<ProcId, ReliableConnection> rcs;
  Delayed<ReliableConnection> loopback_;
  bool connections_moved{false};
  LOGGER_DECL(logger);
};
}  // namespace dory::conn
