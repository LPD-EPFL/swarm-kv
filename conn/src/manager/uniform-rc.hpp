#pragma once

#include <stdexcept>
#include <string>
#include <unordered_map>

#include <dory/ctrl/block.hpp>

#include "../rc.hpp"

namespace dory::conn::manager {

/**
 * Connection Manager where all connections have the same characteristics
 */
template <typename ProcIdType>
class UniformRcConnectionManager {
 public:
  UniformRcConnectionManager(ctrl::ControlBlock &cb) : cb{cb} {}

  void usePd(std::string const &pd) { this->pd = pd; }

  void useMr(std::string const &mr) { this->mr = mr; }

  void useSendCq(std::string const &cq) { this->send_cq = cq; }

  void useRecvCq(std::string const &cq) { this->recv_cq = cq; }

  void setNewConnectionRights(ctrl::ControlBlock::MemoryRights const &rights) {
    this->rights = rights;
  }

  ReliableConnection &newConnection(ProcIdType proc_id,
                                    std::string const &remote_info) {
    if (rcs.find(proc_id) != rcs.end()) {
      throw std::runtime_error("Connection for process " +
                               std::to_string(proc_id) + " already exists!");
    }

    rcs.try_emplace(proc_id, cb);
    auto &rc = rcs.find(proc_id)->second;

    auto remote_rc = RemoteConnection::fromStr(remote_info);

    rc.bindToPd(pd);
    rc.bindToMr(mr);
    rc.associateWithCq(send_cq, recv_cq);

    rc.init(rights);
    rc.connect(remote_rc, proc_id);

    return rc;
  }

  void removeConnection(ProcIdType proc_id) { rcs.erase(proc_id); }

 private:
  std::string pd;
  std::string mr;
  std::string send_cq;
  std::string recv_cq;

  ctrl::ControlBlock::MemoryRights rights;
  ctrl::ControlBlock &cb;

  std::unordered_map<ProcIdType, ReliableConnection> rcs;
};
}  // namespace dory::conn::manager
