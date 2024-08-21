#pragma once

#include <atomic>
#include <cstdint>
#include <fstream>
#include <memory>
#include <sstream>

#include <sys/types.h>  // for pid_t

namespace dory::memory {
class RealTimeHeartbeat {
 public:
  RealTimeHeartbeat(void *location = nullptr);

 private:
  void start();
  static size_t v2p(pid_t pid, uintptr_t vaddr);

  uint64_t *location = nullptr;
  std::ofstream rtcoredev;

  static char constexpr V2pExec[] = "v2p";  // Path to v2p
  static char constexpr DevRtcore[] = "/dev/rtcore";
};
}  // namespace dory::memory

namespace dory::memory {
class PhysicallyLockedBuffer {
 public:
  enum AllocationPool {
    NORMAL,    // 4KiB page
    HUGEPAGE,  // System Default Hugepage (determine with
               // `grep Hugepagesize/proc/meminfo`)
    HUGEPAGE_2MB,
    HUGEPAGE_1GB
  };

  PhysicallyLockedBuffer(size_t length, AllocationPool pool = NORMAL,
                         bool lock = true);
  ~PhysicallyLockedBuffer() noexcept(false);

  void startHeartbeat(size_t offset);
  void *ptr() { return addrptr; }

 private:
  void *addrptr = nullptr;
  size_t length;
  bool locked;
  std::unique_ptr<RealTimeHeartbeat> hb;
  std::atomic<size_t> memfdCounter;
  std::stringstream memfdLocation;
};
}  // namespace dory::memory
