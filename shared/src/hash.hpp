#pragma once

#include <cstddef>
#include <functional>

namespace dory {

// https://stackoverflow.com/questions/2590677/how-do-i-combine-hash-values-in-c0x

inline void hash_combine(std::size_t& /*unused*/) {}

template <typename T, typename... Rest>
inline void hash_combine(std::size_t& seed, const T& v, Rest... rest) {
  std::hash<T> hasher;
  seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  hash_combine(seed, rest...);
}

}  // namespace dory
