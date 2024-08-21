#include <chrono>
#include <iostream>

#include <dory/shared/logger.hpp>

#include "impl.hpp"

auto logger = dory::std_out_logger("MAIN");

#ifdef DALEK_LEGACY
extern "C" {
typedef struct publickey publickey_t;  // NOLINT
typedef struct keypair keypair_t;      // NOLINT

extern uint8_t *public_part(keypair_t *keypair);
extern keypair_t *keypair_create();
extern publickey_t *publickey_new(uint8_t const *key, size_t len);
// extern keypair_t *keypair_new(uint8_t const *key, size_t len);

extern void keypair_free(keypair_t *keypair);
extern void publickey_free(publickey_t *public_key);

extern void keypair_sign_into(uint8_t *sig, keypair_t *keypair,
                              uint8_t const *msg, size_t len);

extern uint8_t publickey_verify_raw(publickey_t *keypair, uint8_t const *msg,
                                    size_t len, uint8_t const *raw_sig);
}
#endif

int main() {
  int iterations = 100000;

  logger->info("Creating and publishing key and verifying own signature");

#ifdef DALEK_LEGACY
  keypair_t *kp = keypair_create();
  if (kp == NULL) {
    printf("keypair_create failed\n");
    return 1;
  }

  publickey_t *pub =
      publickey_new(public_part(kp), crypto_impl::PublicKeyLength);

  uint8_t msg[12];
  size_t msg_len = 12;

  for (int i = 0; i < static_cast<int>(msg_len); i++) {
    msg[i] = static_cast<uint8_t>(i);
  }

  uint8_t sig[crypto_impl::SignatureLength];
  long long sign_microseconds = 0;
  long long verify_microseconds = 0;

  keypair_sign_into(sig, kp, msg, msg_len);

  {
    int successes = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
      successes += publickey_verify_raw(pub, msg, msg_len, sig);
    }
    auto elapsed = std::chrono::high_resolution_clock::now() - start;

    if (successes != iterations) {
      logger->error("Error in verifying ({} vs {})", successes, iterations);
      return 1;
    }

    verify_microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  }

  {
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
      keypair_sign_into(sig, kp, msg, msg_len);
    }
    auto elapsed = std::chrono::high_resolution_clock::now() - start;

    sign_microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  }

  publickey_free(pub);
  keypair_free(kp);

#else
  crypto_impl::init();

  crypto_impl::publish_pub_key_nostore("p1-pk");

  unsigned char sig[crypto_impl::SignatureLength];

  char msg[] = "HELLO WORLD";
  uint64_t msg_len = 12;

  long long sign_microseconds = 0;
  long long verify_microseconds = 0;

  {
    crypto_impl::sign(sig, reinterpret_cast<unsigned char*>(msg), msg_len);

    int successes = 0;

    auto pk = crypto_impl::get_public_key_nostore("p1-pk");

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
      successes += crypto_impl::verify(
          const_cast<unsigned char const*>(sig),
          reinterpret_cast<unsigned char const*>(msg), msg_len, pk);
    }

    if (successes != iterations) {
      logger->error("Error in verifying ({} vs {})", successes, iterations);
      return 1;
    }

    auto elapsed = std::chrono::high_resolution_clock::now() - start;

    verify_microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  }

  {
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
      crypto_impl::sign(sig, reinterpret_cast<unsigned char*>(msg), msg_len);
    }
    auto elapsed = std::chrono::high_resolution_clock::now() - start;

    sign_microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  }
#endif

  logger->info("Verification takes {} us", verify_microseconds / iterations);
  logger->info("Signing takes {} us", sign_microseconds / iterations);

  logger->info("Testing finished successfully!");

  return 0;
}
