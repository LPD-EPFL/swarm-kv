#pragma once

#include <stdexcept>
#include <string>
#include <utility>

#include <dory/shared/host.hpp>
#include <dory/shared/logger.hpp>

#include "../store.hpp"

namespace dory::memstore {
class ProcessAnnouncer {
 public:
  ProcessAnnouncer(bool global_instance = false)
      : LOGGER_INIT(logger, "ProcessAnnouncer") {
    if (!global_instance) {
      auto *st = new MemoryStore("");
      store = dory::deleted_unique_ptr<MemoryStore>(
          st, [](MemoryStore *m) noexcept { delete m; });
    } else {
      store = dory::deleted_unique_ptr<MemoryStore>(
          &MemoryStore::getInstance(),
          [](MemoryStore * /*unused*/) noexcept {});
    }
  }

  template <typename ProcIdType>
  std::pair<std::string, int> processToHost(ProcIdType id) {
    std::string rpc_endpoint;
    if (store->get(Prefix + std::to_string(id), rpc_endpoint)) {
      LOGGER_DEBUG(logger, "Discovered process {} listening at {}", id,
                   rpc_endpoint);
    } else {
      throw std::runtime_error("Process " + std::to_string(id) +
                               " was not found");
    }

    auto colon_pos = rpc_endpoint.find(':');
    std::string hostname = rpc_endpoint.substr(0, colon_pos);
    int port = std::stoi(rpc_endpoint.substr(colon_pos + 1));

    return std::make_pair(ip_address(hostname), port);
  }

  template <typename ProcIdType>
  void announceProcess(ProcIdType id, int port) {
    std::string listening(fq_hostname() + ":" + std::to_string(port));

    LOGGER_DEBUG(logger, "Announcing process {} listening at {}", id,
                 listening);
    store->set(Prefix + std::to_string(+id), listening);
  }

 private:
  dory::deleted_unique_ptr<MemoryStore> store;
  static auto constexpr Prefix = "PID-";
  LOGGER_DECL(logger);
};
}  // namespace dory::memstore
