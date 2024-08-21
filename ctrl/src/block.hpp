#pragma once

#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <dory/extern/ibverbs.hpp>
#include <dory/memory/locked-memory.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/pointer-wrapper.hpp>
#include <dory/shared/types.hpp>
#include "device.hpp"

namespace dory::ctrl {
class ControlBlock {
 public:
  /**
   * Memory access attributes by the RDMA device.
   *
   * If `REMOTE_WRITE` is set, then `LOCAL_WRITE` must be set too,
   * since remote write should be allowed only if local write is allowed.
   **/
  enum MemoryRights {
    LOCAL_READ = 0,
    LOCAL_WRITE = IBV_ACCESS_LOCAL_WRITE,
    REMOTE_READ = IBV_ACCESS_REMOTE_READ,
    REMOTE_WRITE = IBV_ACCESS_REMOTE_WRITE,
    REMOTE_ATOMIC = IBV_ACCESS_REMOTE_ATOMIC,
  };

  struct MemoryRegion {
    uintptr_t addr;
    uint64_t size;
    uint32_t lkey;
    uint32_t rkey;
  };

#ifdef DORY_CTRL_DM
  struct DeviceMemory {
    DeviceMemory(std::shared_ptr<struct ibv_dm> &dm, size_t size)
        : dm(dm), size(size) {}

    inline int copyTo(uptrdiff_t offset, void const *src, size_t length) {
      return ibv_memcpy_to_dm(dm.get(), offset, src, length);
    }

    template <typename T>
    inline int copyTo(uptrdiff_t offset, T &src) {
      return copyTo(offset, &src, sizeof(T));
    }

    int copyFrom(uptrdiff_t offset, void *dest, size_t length) const {
      return ibv_memcpy_from_dm(dest, dm.get(), offset, length);
    }

    template <typename T>
    inline int copyFrom(uptrdiff_t offset, T &dest) const {
      return copyFrom(offset, &dest, sizeof(T));
    }

    std::shared_ptr<struct ibv_dm> dm;
    size_t size;
  };
#endif

  static int constexpr CqDepth = 512;

  ControlBlock(ResolvedPort &resolved_port);

  void registerPd(std::string const &name);

  deleted_unique_ptr<struct ibv_pd> &pd(std::string const &name);

  void allocateBuffer(std::string const &name, size_t length, size_t alignment);
  void allocatePhysicallyLockedBuffer(
      std::string const &name, size_t length,
      memory::PhysicallyLockedBuffer::AllocationPool allocation_pool);

#ifdef DORY_CTRL_DM
  void allocateDm(std::string const &name, size_t length, size_t alignment);
#endif

  void registerMr(std::string const &name, std::string const &pd_name,
                  std::string const &buffer_name, size_t offset, size_t buf_len,
                  MemoryRights rights);

  void registerMr(std::string const &name, std::string const &pd_name,
                  std::string const &buffer_name,
                  MemoryRights rights = LOCAL_READ);

#ifdef DORY_CTRL_DM
  void registerDmMr(std::string const &name, std::string const &pd_name,
                    std::string const &dm_name, size_t offset, size_t buf_len,
                    MemoryRights rights);
  void registerDmMr(std::string const &name, std::string const &pd_name,
                    std::string const &dm_name,
                    MemoryRights rights = LOCAL_READ);
  DeviceMemory dm(std::string const &name) const;
#endif

  // void withdrawMrRight(std::string name) const;
  MemoryRegion mr(std::string const &name) const;

  void registerCq(std::string const &name);
  deleted_unique_ptr<struct ibv_cq> &cq(std::string const &name);

  uint8_t port() const;
  uint16_t lid() const;

  static bool pollCqIsOk(deleted_unique_ptr<struct ibv_cq> &cq,
                         std::vector<struct ibv_wc> &entries);

 private:
  ResolvedPort resolved_port;

  std::map<std::string, deleted_unique_ptr<struct ibv_pd>> pds;

  // std::unique_ptr is semantically more suitable, but it is not polymorphic
  // when using different deleters. We rely on the other unique_ptr's of this
  // class to prevent it from being copyable.
  std::vector<std::shared_ptr<uint8_t>> raw_bufs;
  std::map<std::string, std::pair<size_t, size_t>> buf_map;

#ifdef DORY_CTRL_DM
  std::map<std::string, DeviceMemory> dms;
#endif

  std::map<std::string, deleted_unique_ptr<struct ibv_mr>> mrs;

  std::map<std::string, deleted_unique_ptr<struct ibv_cq>> cqs;

  LOGGER_DECL(logger);
};

inline constexpr ControlBlock::MemoryRights operator|(
    ctrl::ControlBlock::MemoryRights a, ControlBlock::MemoryRights b) {
  using Scalar = std::underlying_type_t<ControlBlock::MemoryRights>;
  return static_cast<ControlBlock::MemoryRights>(static_cast<Scalar>(a) |
                                                 static_cast<Scalar>(b));
}
}  // namespace dory::ctrl
