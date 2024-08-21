#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include <sys/prctl.h>
#include <sys/utsname.h>

#include <dory/shared/logger.hpp>

#include "heartbeat.hpp"

#ifndef PR_SET_HEARTBEAT
#define PR_SET_HEARTBEAT 57
#endif

namespace dory::special {

auto logger = dory::std_out_logger("SPECIAL");

void enable_heartbeat(int data) {
  struct utsname buf;

  // Check the kernel version
  auto ret_uname = uname(&buf);
  if (ret_uname == -1) {
    throw std::runtime_error("Could not read the kernel version (" +
                             std::to_string(errno) +
                             "): " + std::string(std::strerror(errno)));
  }

  bool found = false;
  for (const auto& current : heartbeat::compatible_kernels) {
    if (std::string(buf.release) != std::string(current.release)) {
      continue;
    }

    if (std::string(buf.machine) != std::string(current.arch)) {
      continue;
    }

    std::size_t found_version =
        std::string(buf.version).find(std::string(current.version));
    if (found_version == std::string::npos) {
      continue;
    }

    found = true;
  }

  if (!found) {
    LOGGER_WARN(
        logger,
        "The current kernel is not compatible with any of the supported "
        "kernels. The heartbeat mechanism is not supported");
    return;
  }

  // Try to enable the heartbeat mechanism
  auto ret_prctl = prctl(PR_SET_HEARTBEAT, data, 0, 0, 0);
  if (ret_prctl != 0) {
    throw std::runtime_error(
        "Could not use prctl to enable the heartbeat for this process (" +
        std::to_string(errno) + "): " + std::string(std::strerror(errno)));
  }
}
}  // namespace dory::special
