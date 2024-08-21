#pragma once

#include <array>
#include <vector>

#include <dory/third-party/blake3/blake3.h>
#include <dory/shared/concepts.hpp>

namespace dory::crypto::hash {
static constexpr size_t Blake3HashLength = BLAKE3_OUT_LEN;

using Blake3Hash = std::array<uint8_t, Blake3HashLength>;

// Multi-part

using Blake3Hasher = blake3_hasher;

static inline Blake3Hasher blake3_init() {
  Blake3Hasher state;
  blake3_hasher_init(&state);
  return state;
}

static inline void blake3_update(Blake3Hasher &state,
                                 uint8_t const *const begin,
                                 uint8_t const *const end) {
  blake3_hasher_update(&state, begin, static_cast<size_t>(end - begin));
}

template <typename ContiguousIt,
          concepts::IsRandomIterator<ContiguousIt> = true>
static inline void blake3_update(Blake3Hasher &state, ContiguousIt begin,
                                 ContiguousIt end) {
  blake3_update(state, reinterpret_cast<uint8_t const *const>(&*begin),
                reinterpret_cast<uint8_t const *const>(&*end));
}

static inline void blake3_update(
    Blake3Hasher &state, std::vector<Blake3Hash::value_type> const &message) {
  blake3_update(state, message.begin(), message.end());
}

template <typename T, concepts::IsTrivial<T> = true>
static inline void blake3_update(Blake3Hasher &state, T const &value) {
  auto const *const begin = reinterpret_cast<uint8_t const *const>(&value);
  blake3_update(state, begin, begin + sizeof(T));
}

static inline Blake3Hash blake3_final(Blake3Hasher &state) {
  Blake3Hash ret;
  blake3_hasher_finalize(&state, ret.data(), Blake3HashLength);
  return ret;
}

// Single-part

static inline Blake3Hash blake3(uint8_t const *const begin,
                                uint8_t const *const end) {
  auto state = blake3_init();
  blake3_update(state, begin, end);
  return blake3_final(state);
}

template <typename ContiguousIt,
          concepts::IsRandomIterator<ContiguousIt> = true>
static inline Blake3Hash blake3(ContiguousIt begin, ContiguousIt end) {
  return blake3(reinterpret_cast<uint8_t const *const>(&*begin),
                reinterpret_cast<uint8_t const *const>(&*end));
}

static inline Blake3Hash blake3(
    std::vector<Blake3Hash::value_type> const &message) {
  return blake3(message.begin(), message.end());
}

template <typename T, concepts::IsTrivial<T> = true>
static inline Blake3Hash blake3(T const &value) {
  auto const *const begin = reinterpret_cast<uint8_t const *const>(&value);
  return blake3(begin, begin + sizeof(T));
}

}  // namespace dory::crypto::hash
