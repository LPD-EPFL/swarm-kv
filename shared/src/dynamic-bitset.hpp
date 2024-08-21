#pragma once

#include <array>
#include <cstddef>
#include <stdexcept>

#include "branching.hpp"

namespace dory {
class DynamicBitset {
  static size_t constexpr MaxCapacity = 8;

 public:
  inline DynamicBitset(size_t const capacity) : bits{}, capacity{capacity} {
    if (unlikely(capacity > MaxCapacity)) {
      throw std::invalid_argument("Max capacity violated.");
    }
  }

  DynamicBitset(DynamicBitset const&) = delete;
  DynamicBitset& operator=(DynamicBitset const&) = delete;
  DynamicBitset(DynamicBitset&&) = default;
  DynamicBitset& operator=(DynamicBitset&&) = default;

  inline bool set(size_t const index) {
    auto& ref = bits[index];
    if (ref) {
      return false;
    }
    ref = true;
    _size++;
    return true;
  }

  inline bool get(size_t const index) const { return bits[index]; }

  inline bool empty() const { return _size == 0; }

  inline bool full() const { return _size == capacity; }

  inline bool majority() const { return _size >= capacity / 2 + 1; }

  size_t size() const { return _size; }

 private:
  std::array<bool, MaxCapacity> bits;
  size_t capacity;
  size_t _size = 0;
};
}  // namespace dory
