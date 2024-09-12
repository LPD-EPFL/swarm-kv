#pragma once

#include <array>
#include <chrono>
#include <functional>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <fmt/chrono.h>
#include <xxhash.h>

#include <fmt/color.h>
#include <fmt/ranges.h>

#include <dory/shared/branching.hpp>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/memstore/store.hpp>
#include <dory/shared/pinning.hpp>
#include <dory/shared/units.hpp>

#include <dory/extern/ibverbs.hpp>

#include "base.hpp"
#include "race/buckets.hpp"

using ConnectionExchanger = dory::conn::RcConnectionExchanger<ProcId>;

const static auto cleanline = "\33[2K\r";
const static int64_t loop_detect = 100'000;

static HashedKey hash(KeyView const& key) {
  auto const h = XXH3_128bits(key.data(), key.size());
  return *reinterpret_cast<HashedKey const*>(&h);
}

template <>
struct std::hash<HashedKey> {
  std::size_t operator()(const HashedKey& k) const noexcept {
    return reinterpret_cast<uint64_t const*>(&k)[1];
  }
};

static std::stringstream exec(const std::string& cmd) {
  std::array<char, 128> buffer;
  std::stringstream output;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                pclose);
  if (!pipe) throw std::runtime_error("popen() failed!");
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
    output << buffer.data();
  return output;
}