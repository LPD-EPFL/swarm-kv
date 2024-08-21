#pragma once

#include <cstddef>

#include "constants.hpp"

namespace dory::race {
using ProcId = int;

using Value = uintptr_t;
size_t constexpr ValueMask = (1ull << 48) - 1;

struct BucketEntry {
  using Checksum = uint8_t;


  BucketEntry(Checksum checksum, Value value): used{1}, padding{0}, checksum{checksum}, value{value & ValueMask} {}
  
  uint64_t as_uint64() const {
    return *reinterpret_cast<uint64_t const*>(this);
  }
  
  bool operator==(BucketEntry const& o) {
    return as_uint64() == o.as_uint64();
  }
  Value get_value() const {
    return value & ValueMask;
  }

  bool used: 1;
  uint64_t padding: 7;
  Checksum checksum: 8;
  Value value: 48;
};
static_assert(sizeof(BucketEntry) == sizeof(uint64_t));
using Bucket = std::array<BucketEntry, NbBucketEntries>;
}