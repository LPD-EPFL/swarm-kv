#include "sodium.hpp"
#include "map.hpp"

#include <memory>

#include <thread>

#include <dory/shared/logger.hpp>
#include <dory/shared/pointer-wrapper.hpp>

namespace dory::crypto::asymmetric::sodium {

auto logger = dory::std_out_logger("CRYPTO");
ThreadSafeMap<std::string, std::string> nostore_map;

unsigned char own_pk[crypto_sign_PUBLICKEYBYTES];
unsigned char own_sk[crypto_sign_SECRETKEYBYTES];

volatile bool initialized = false;

void init() {
  if (initialized) {
    SPDLOG_LOGGER_WARN(logger, "Trying to re-initialize the sodium library!");
    return;
  }

  initialized = true;

  if (sodium_init() == -1) {
    throw std::runtime_error("Initializing libsodium failed.");
  }

  crypto_sign_keypair(own_pk, own_sk);
}

void publish_pub_key(std::string const& mem_key) {
  dory::memstore::MemoryStore::getInstance().set(
      mem_key,
      std::string(reinterpret_cast<char*>(own_pk), crypto_sign_PUBLICKEYBYTES));
}

void publish_pub_key_nostore(std::string const& mem_key) {
  nostore_map.set(mem_key, std::string(reinterpret_cast<char*>(own_pk),
                                       crypto_sign_PUBLICKEYBYTES));
}

pub_key get_public_key(std::string const& mem_key) {
  std::string ret;

  if (!dory::memstore::MemoryStore::getInstance().get(mem_key, ret)) {
    throw std::runtime_error("Key not found");
  }

  auto* rpk =
      reinterpret_cast<unsigned char*>(malloc(crypto_sign_PUBLICKEYBYTES));

  ret.copy(reinterpret_cast<char*>(rpk), crypto_sign_PUBLICKEYBYTES, 0);

  return deleted_unique_ptr<unsigned char>(
      rpk, [](unsigned char* data) noexcept { free(data); });
}

pub_key get_public_key_nostore(std::string const& mem_key) {
  auto ret = nostore_map.get(mem_key);

  if (!ret) {
    throw std::runtime_error("Key not found");
  }

  auto* rpk =
      reinterpret_cast<unsigned char*>(malloc(crypto_sign_PUBLICKEYBYTES));

  ret->copy(reinterpret_cast<char*>(rpk), crypto_sign_PUBLICKEYBYTES, 0);

  return deleted_unique_ptr<unsigned char>(
      rpk, [](unsigned char* data) noexcept { free(data); });
}

std::map<int, pub_key> get_public_keys(std::string const& prefix,
                                       std::vector<int> const& remote_ids) {
  std::map<int, pub_key> remote_keys;

  for (int pid : remote_ids) {
    auto memkey = prefix + std::to_string(pid);
    while (true) {
      try {
        remote_keys.insert(
            std::pair<int, pub_key>(pid, get_public_key(memkey)));
        break;
      } catch (...) {
        logger->info("{} not pushlished yet", memkey);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
  }

  return remote_keys;
}

int sign(unsigned char* sig, unsigned char const* msg, uint64_t msg_len) {
  return crypto_sign_detached(sig, nullptr, msg, msg_len, own_sk);
}

bool verify(unsigned char const* sig, unsigned char const* msg,
            uint64_t msg_len, pub_key const& pk) {
  return crypto_sign_verify_detached(sig, msg, msg_len, pk.get()) == 0;
}

}  // namespace dory::crypto::asymmetric::sodium
