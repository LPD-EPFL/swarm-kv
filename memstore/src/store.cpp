#include <unistd.h>
#include <cstring>
#include <regex>
#include <stdexcept>
#include <thread>

#include "store.hpp"

namespace dory::memstore {
MemoryStore::MemoryStore() : memc(memcached_create(nullptr), memcached_free) {
  if (memc.get() == nullptr) {
    throw std::runtime_error("Failed to create memcached handle");
  }

  auto [ip, port] = ipPortFromEnvVar(RegIPName);
  memcached_return_t rc;

  deleted_unique_ptr<memcached_server_st> servers(
      memcached_server_list_append(nullptr, ip.c_str(), port, &rc),
      memcached_server_list_free);

  auto push_ret = memcached_server_push(memc.get(), servers.get());
  if (push_ret != MEMCACHED_SUCCESS) {
    throw std::runtime_error(
        "Could not add memcached server in the MemoryStore: " +
        std::string(memcached_strerror(memc.get(), push_ret)));
  }

  rc =
      memcached_behavior_set(memc.get(), MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);

  if (rc != MEMCACHED_SUCCESS) {
    throw std::runtime_error("Could not switch to the binary protocol: " +
                             std::string(memcached_strerror(memc.get(), rc)));
  }
}

MemoryStore::MemoryStore(std::string const &prefix_) : MemoryStore() {
  prefix = prefix_;
}

void MemoryStore::set(std::string const &key, std::string const &value) {
  if (key.length() == 0 || value.length() == 0) {
    throw std::runtime_error("Empty key or value");
  }

  // Check if key already exists. This error indicates a potential for naming
  // collision when announcing RDMA resources.
  std::string dummy;
  if (get(key, dummy)) {
    throw std::runtime_error("Trying to set key `" + key +
                             "` that already exists");
  }

  memcached_return_t rc;
  std::string prefixed_key = prefix + key;
  rc = memcached_set(memc.get(), prefixed_key.c_str(), prefixed_key.length(),
                     value.c_str(), value.length(), static_cast<time_t>(0),
                     static_cast<uint32_t>(0));

  if (rc != MEMCACHED_SUCCESS) {
    throw std::runtime_error(
        "Failed to set to the store the (K, V) = (" + key + ", " + value +
        ") (" + std::string(memcached_strerror(memc.get(), rc)) + ")");
  }
}

bool MemoryStore::get(std::string const &key, std::string &value) {
  if (key.length() == 0) {
    throw std::runtime_error("Empty key");
  }

  memcached_return_t rc;
  size_t value_length;
  uint32_t flags;

  std::string prefixed_key = prefix + key;
  char *ret_value =
      memcached_get(memc.get(), prefixed_key.c_str(), prefixed_key.length(),
                    &value_length, &flags, &rc);
  deleted_unique_ptr<char> ret_value_uniq(ret_value, free);

  if (rc == MEMCACHED_SUCCESS) {
    std::string ret(ret_value, value_length);
    value += ret;
    return true;
  }
  if (rc == MEMCACHED_NOTFOUND) {
    return false;
  }
  throw std::runtime_error(
      "Failed to get from the store the K = " + key + " (" +
      std::string(memcached_strerror(memc.get(), rc)) + ")");
  // Never reached
  return false;
}

bool MemoryStore::remove(std::string const &key) {
  if (key.length() == 0) {
    throw std::runtime_error("Empty key");
  }

  memcached_return_t rc;
  std::string prefixed_key = prefix + key;
  rc = memcached_delete(memc.get(), prefixed_key.c_str(), prefixed_key.length(),
                        static_cast<time_t>(0));

  if (rc == MEMCACHED_NOTFOUND) {
    return false;
  }

  if (rc != MEMCACHED_SUCCESS) {
    throw std::runtime_error(
        "Failed to delete rom the store the K = " + key + " (" + std::string(memcached_strerror(memc.get(), rc)) + ")");
  }

  return true;
}

void MemoryStore::barrier(std::string const &key, size_t const wait_for) {
  uint64_t ret_val = 0;

  uint64_t const initial_val = 1;
  uint64_t incr_val = 1;
  time_t const expiration_time = 0;

  while (ret_val < wait_for) {
    auto const rc = memcached_increment_with_initial(
        memc.get(), key.c_str(), key.size(), incr_val, initial_val,
        expiration_time, &ret_val);

    if (rc != MEMCACHED_SUCCESS) {
      if (rc == MEMCACHED_NOTSTORED) {
        std::this_thread::sleep_for(RetryTime);
        continue;
      }

      throw std::runtime_error("Failed to atomically increment: " +
                               std::string(memcached_strerror(memc.get(), rc)));
    }

    incr_val = 0;

    if (ret_val != wait_for) {
      std::this_thread::sleep_for(RetryTime);
    }
  }

  if (ret_val > wait_for) {
    throw std::runtime_error("The barrier with key `" + key +
                             "` exceeded its wait_for argument (" +
                             std::to_string(ret_val) + " instead of " +
                             std::to_string(wait_for) + ")");
  }
}

std::pair<std::string, uint16_t> MemoryStore::ipPortFromEnvVar(
    char const *const name) {
  char const *env = getenv(name);
  if (env == nullptr) {
    throw std::runtime_error("Environment variable " + std::string(name) +
                             " not set");
  }

  std::string s(env);
  std::regex regex(":");

  std::vector<std::string> split_string(
      std::sregex_token_iterator(s.begin(), s.end(), regex, -1),
      std::sregex_token_iterator());

  switch (split_string.size()) {
    case 0:
      throw std::runtime_error("Environment variable " + std::string(name) +
                               " contains insufficient data");
      break;

    case 1:
      return std::make_pair(split_string[0], MemcacheDDefaultPort);
    case 2:
      return std::make_pair(split_string[0], stoi(split_string[1]));

    default:
      throw std::runtime_error("Environment variable " + std::string(name) +
                               " contains excessive data");
  }

  // Unreachable
  return std::make_pair("", 0);
}

}  // namespace dory::memstore
