#pragma once

#include <array>
#include <vector>

#include <sodium.h>

#include <dory/shared/concepts.hpp>

#include "../internal/check.hpp"

namespace dory::crypto::hash {
static constexpr size_t Blake2HashLength = crypto_generichash_BYTES;

using Blake2Hash =
    std::array<uint8_t, Blake2HashLength>;  // uint8_t[Blake2HashLength];

// Single-part

static inline Blake2Hash blake2b(uint8_t const *const begin,
                                 uint8_t const *const end) {
  Blake2Hash ret;
  internal::libsodium_check(
      crypto_generichash(ret.data(), Blake2HashLength, begin,
                         static_cast<size_t>(end - begin), nullptr, 0));
  return ret;
}

template <typename ContiguousIt,
          concepts::IsRandomIterator<ContiguousIt> = true>
static inline Blake2Hash blake2b(ContiguousIt begin, ContiguousIt end) {
  // assert(&*end - &*first == end - first && "Iterators must represent a
  // contiguous memory region");
  return blake2b(reinterpret_cast<uint8_t const *const>(&*begin),
                 reinterpret_cast<uint8_t const *const>(&*end));
}

static inline Blake2Hash blake2b(
    std::vector<Blake2Hash::value_type> const &message) {
  return blake2b(message.begin(), message.end());
}

template <typename T, concepts::IsTrivial<T> = true>
static inline Blake2Hash blake2b(T const &value) {
  auto const *const begin = reinterpret_cast<uint8_t const *const>(&value);
  return blake2b(begin, begin + sizeof(T));
}

// Multi-part

using Blake2Hasher = crypto_generichash_state;

static inline Blake2Hasher blake2b_init() {
  Blake2Hasher state;
  crypto_generichash_init(&state, nullptr, 0, Blake2HashLength);
  return state;
}

static inline void blake2b_update(Blake2Hasher &state,
                                  uint8_t const *const begin,
                                  uint8_t const *const end) {
  internal::libsodium_check(crypto_generichash_update(
      &state, begin, static_cast<size_t>(end - begin)));
}

template <typename ContiguousIt,
          concepts::IsRandomIterator<ContiguousIt> = true>
static inline void blake2b_update(Blake2Hasher &state, ContiguousIt begin,
                                  ContiguousIt end) {
  blake2b_update(state, reinterpret_cast<uint8_t const *const>(&*begin),
                 reinterpret_cast<uint8_t const *const>(&*end));
}

static inline void blake2b_update(
    Blake2Hasher &state, std::vector<Blake2Hash::value_type> const &message) {
  blake2b_update(state, message.begin(), message.end());
}

template <typename T, concepts::IsTrivial<T> = true>
static inline void blake2b_update(Blake2Hasher &state, T const &value) {
  auto const *const begin = reinterpret_cast<uint8_t const *const>(&value);
  blake2b_update(state, begin, begin + sizeof(T));
}

static inline Blake2Hash blake2b_final(Blake2Hasher &state) {
  Blake2Hash ret;
  crypto_generichash_final(&state, ret.data(), Blake2HashLength);
  return ret;
}

}  // namespace dory::crypto::hash
