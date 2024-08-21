#pragma once

#include <stdexcept>

namespace dory::crypto::internal {
static void libsodium_check(int ret) {
  if (ret != 0) {
    throw std::runtime_error("Libsodium failed");
  }
}
}  // namespace dory::crypto::internal
