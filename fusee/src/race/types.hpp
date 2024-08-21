#pragma once

#include "../main.hpp"
#include "common.hpp"
#include <cstddef>

namespace dory::race {

using Value = uintptr_t;
size_t constexpr ValueMask = (1ULL << 48) - 1;

struct BucketEntry {
  using Checksum = uint8_t;

  BucketEntry(Checksum checksum, Value value)
      : used{true}, padding{0}, checksum{checksum}, value{value & ValueMask} {}

  BucketEntry() : used{false}, padding{0}, checksum{0}, value{0} {}

  uint64_t asUint64() const { return *reinterpret_cast<uint64_t const*>(this); }

  bool operator==(BucketEntry const& o) const {
    return asUint64() == o.asUint64();
  }
  Value getValue() const { return value & ValueMask; }

  bool used: 1;
  uint64_t padding: 7;
  Checksum checksum: 8;
  Value value: 48;
};
static_assert(sizeof(BucketEntry) == sizeof(uint64_t));
using Bucket = std::array<BucketEntry, NbBucketEntries>;
using BucketGroup = std::array<Bucket, 2>;
}  // namespace dory::race