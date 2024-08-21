#pragma once

#include <chrono>
#include <cstdint>
#include <dory/extern/memcached.hpp>
#include <dory/shared/pointer-wrapper.hpp>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace dory::memstore {
/**
 * This class acts as a central public registry for all processes.
 * It provides a lazy initialized singleton instance.
 */
class MemoryStore {
 public:
  /**
   * Getter for the singleton instance.
   * @return  MemoryStore
   * */
  static MemoryStore &getInstance() {
    static MemoryStore instance;

    return instance;
  }

  MemoryStore(std::string const &prefix);

  /**
   * Stores the provided string `value` under `key`.
   * @param key
   * @param value
   * @throw `runtime_error`
   */
  void set(std::string const &key, std::string const &value);

  /**
   * Gets the value associated with `key` and appends it to `value`.
   * @param key
   * @param value
   * @return bool indicating the success
   * @throw `runtime_error`
   */
  bool get(std::string const &key, std::string &value);

  /**
   * Removes the value associated with `key`.
   * @param key
   * @return bool indicating the success
   * @throw `runtime_error`
   */
  bool remove(std::string const &key);

  /**
   * Atomically increments a value and waits for it to reach `wait_for` before
   * returning. If the key does not exist, it is automatically created and it is
   * set to 0.
   * @param key
   * @param wait_for
   * @throw `runtime_error`
   */
  void barrier(std::string const &key, size_t wait_for);

 private:
  MemoryStore();

  static std::pair<std::string, uint16_t> ipPortFromEnvVar(char const *name);
  static auto constexpr RegIPName = "DORY_REGISTRY_IP";
  static auto constexpr MemcacheDDefaultPort = MEMCACHED_DEFAULT_PORT;  // 11211
  static auto constexpr RetryTime = std::chrono::milliseconds(20);

  deleted_unique_ptr<memcached_st> memc;
  std::string prefix;
};
}  // namespace dory::memstore

// Re-export
#include "internal/announcer.hpp"
namespace dory::memstore {
class ProcessAnnouncer;
}
