
#include "dalek.hpp"
#include "map.hpp"

#include <cstdint>
#include <iostream>
#include <memory>
#include <thread>

#include <dory/shared/logger.hpp>
#include <dory/shared/pointer-wrapper.hpp>

namespace dory::crypto::asymmetric::dalek {

extern "C" {

typedef struct keypair keypair_t;  // NOLINT

extern uint8_t *public_part(keypair_t *keypair);
extern keypair_t *keypair_create();
extern publickey_t *publickey_new(uint8_t const *key, size_t len);
extern keypair_t *keypair_new(uint8_t const *key, size_t len);

extern void keypair_free(keypair_t *keypair);
extern void publickey_free(publickey_t *public_key);

extern void keypair_sign_into(uint8_t *sig, keypair_t *keypair,
                              uint8_t const *msg, size_t len);

extern dory::crypto::asymmetric::dalek::signature keypair_sign(
    keypair_t *keypair, uint8_t const *msg, size_t len);

extern uint8_t keypair_verify(keypair_t *keypair, uint8_t const *msg,
                              size_t len,
                              dory::crypto::asymmetric::dalek::signature *sig);

extern uint8_t publickey_verify_raw(publickey_t *keypair, uint8_t const *msg,
                                    size_t len, uint8_t const *raw_sig);
extern uint8_t publickey_verify(
    publickey_t *public_key, uint8_t const *msg, size_t len,
    dory::crypto::asymmetric::dalek::signature const *sig);
}

auto logger = dory::std_out_logger("CRYPTO");
ThreadSafeMap<std::string, std::string> nostore_map;

deleted_unique_ptr<keypair> kp;

volatile bool initialized = false;

void init() {
  if (initialized) {
    SPDLOG_LOGGER_WARN(logger, "Trying to re-initialize dalek's library!");
    return;
  }

  initialized = true;

  auto *raw_kp = keypair_create();

  kp = deleted_unique_ptr<keypair>(raw_kp,
                                   [](keypair *rkp) { keypair_free(rkp); });
}

void publish_pub_key(std::string const &mem_key) {
  dory::memstore::MemoryStore::getInstance().set(
      mem_key, std::string(reinterpret_cast<char *>(public_part(kp.get())),
                           PublicKeyLength));
}

void publish_pub_key_nostore(std::string const &mem_key) {
  nostore_map.set(mem_key,
                  std::string(reinterpret_cast<char *>(public_part(kp.get())),
                              PublicKeyLength));
}

pub_key get_public_key(std::string const &mem_key) {
  std::string ret;

  if (!dory::memstore::MemoryStore::getInstance().get(mem_key, ret)) {
    throw std::runtime_error("Key not found");
  }

  return deleted_unique_ptr<publickey>(
      publickey_new(reinterpret_cast<uint8_t *>(ret.data()), PublicKeyLength),
      [](publickey *rpk) { publickey_free(rpk); });
}

pub_key get_public_key_nostore(std::string const &mem_key) {
  auto ret = nostore_map.get(mem_key);

  if (!ret) {
    throw std::runtime_error("Key not found");
  }

  return deleted_unique_ptr<publickey>(
      publickey_new(reinterpret_cast<uint8_t *>((*ret).data()),
                    PublicKeyLength),
      [](publickey *rpk) { publickey_free(rpk); });
}

std::map<int, pub_key> get_public_keys(std::string const &prefix,
                                       std::vector<int> const &remote_ids) {
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

signature sign(unsigned char const *msg, uint64_t msg_len) {
  return keypair_sign(kp.get(), msg, msg_len);
}

void sign(unsigned char *buf, unsigned char const *msg, uint64_t msg_len) {
  return keypair_sign_into(buf, kp.get(), msg, msg_len);
}

bool verify(signature const &sig, unsigned char const *msg, uint64_t msg_len,
            pub_key &pk) {
  return publickey_verify(pk.get(), msg, msg_len, &sig);
}

bool verify(unsigned char const *sig, unsigned char const *msg,
            uint64_t msg_len, pub_key &pk) {
  return publickey_verify_raw(pk.get(), msg, msg_len,
                              reinterpret_cast<uint8_t const *>(sig));
}

}  // namespace dory::crypto::asymmetric::dalek
