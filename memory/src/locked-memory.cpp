#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "locked-memory.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <cstdio>

#include <linux/memfd.h>
#include <sys/mman.h>

// Linux specific HUGE_PAGE config
#include <asm-generic/hugetlb_encode.h>
#define MAP_HUGE_2MB HUGETLB_FLAG_ENCODE_2MB
#define MAP_HUGE_1GB HUGETLB_FLAG_ENCODE_1GB

#include <unistd.h>
#include <cstdint>
#include <iostream>

#include "internal/heartbeat.hpp"

namespace dory::memory {

RealTimeHeartbeat::RealTimeHeartbeat(void *loc)
    : location{static_cast<uint64_t *>(loc)} {
  rtcoredev.rdbuf()->pubsetbuf(nullptr, 0);
  start();
}

void RealTimeHeartbeat::start() {
  auto vaddr = reinterpret_cast<uintptr_t>(location);
  auto paddr = v2p(getpid(), vaddr);
  std::cout << "Physical address: " << std::hex << paddr << std::endl;

  rtcoredev.open(DevRtcore);

  // Perform the write in one shot, by writing to a stringstream first
  std::stringstream ss;
  ss << "1 "
     << "0x" << std::hex << paddr << "\n";
  rtcoredev << ss.str();

  if (!rtcoredev.good()) {
    throw std::runtime_error("Could not start the RT core");
  }
}

size_t RealTimeHeartbeat::v2p(pid_t pid, uintptr_t vaddr) {
  // Make the page dirty, otherwise the physical address will resolve to 0x0
  asm volatile("mfence" ::: "memory");
  size_t volatile *dirty_page = reinterpret_cast<size_t *>(vaddr);
  dirty_page[0] = dirty_page[0];
  asm volatile("mfence" ::: "memory");

  std::stringstream ss;
  ss << V2pExec << " --pid " << pid << " --address "
     << "0x" << std::hex << vaddr << " 2>&1";
  //  << "0x" << std::hex << vaddr << "--start 2>&1";

  auto *cmd_stream = popen(ss.str().c_str(), "r");
  if (cmd_stream == nullptr) {
    throw std::runtime_error("Could not invoke v2p (" + std::to_string(errno) +
                             "): " + std::string(std::strerror(errno)));
  }

  char buf[128];
  std::string cmd_output;

  /* Read the output a line at a time - output it. */
  while (fgets(buf, sizeof(buf), cmd_stream) != nullptr) {
    cmd_output.append(buf);
  }

  if (ferror(cmd_stream)) {
    throw std::runtime_error("Error when fetch the output of v2p");
  }

  auto ret = pclose(cmd_stream);
  if (ret == -1) {
    throw std::runtime_error("Could not return from the invocation of v2p (" +
                             std::to_string(errno) +
                             "): " + std::string(std::strerror(errno)));
  }

  if (WEXITSTATUS(ret) != 0) {
    throw std::runtime_error("v2p exited with error (" +
                             std::to_string(WEXITSTATUS(ret)) +
                             "): " + cmd_output);
  }

  return std::strtoull(cmd_output.c_str(), nullptr, 0);
}
}  // namespace dory::memory

namespace dory::memory {
PhysicallyLockedBuffer::PhysicallyLockedBuffer(size_t length,
                                               AllocationPool pool, bool lock)
    : length{length}, locked{lock}, memfdCounter{0} {
  // int flags = MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE;
  int flags = MAP_SHARED | MAP_NORESERVE;
  unsigned int memfd_flags = 0;

  switch (pool) {
    case HUGEPAGE:
      flags |= MAP_HUGETLB;
      memfd_flags |= MFD_HUGETLB;
      break;
    case HUGEPAGE_2MB:
      flags |= MAP_HUGETLB | MAP_HUGE_2MB;
      memfd_flags |= MFD_HUGETLB | MFD_HUGE_2MB;
      break;
    case HUGEPAGE_1GB:
      flags |= MAP_HUGETLB | MAP_HUGE_1GB;
      memfd_flags |= MFD_HUGETLB | MFD_HUGE_1GB;
      break;
    default:
      break;
  }

  if (lock) {
    flags |= MAP_LOCKED;
  }

  std::string memfd_name =
      "PhysicallyLockedBuffer-" + std::to_string(memfdCounter.fetch_add(1));
  auto memfd = memfd_create(memfd_name.c_str(), memfd_flags);
  if (memfd == -1) {
    throw std::runtime_error(
        "Could not create the file-backed memory mapping (" +
        std::to_string(errno) + "): " + std::string(std::strerror(errno)));
  }

  memfdLocation << "/proc/" << getpid() << "/fd/" << memfd;

  if (ftruncate(memfd, static_cast<off_t>(length)) == -1) {
    throw std::runtime_error(
        "Could not set the proper length to the "
        "file-backed memory mapping (" +
        std::to_string(errno) + "): " + std::string(std::strerror(errno)));
  }

  addrptr = mmap(nullptr, length, PROT_READ | PROT_WRITE, flags, memfd, 0);

  if (addrptr == MAP_FAILED) {
    throw std::runtime_error("Could not create the memory mapping (" +
                             std::to_string(errno) +
                             "): " + std::string(std::strerror(errno)));
  }
}

PhysicallyLockedBuffer::~PhysicallyLockedBuffer() noexcept(false) {
  // Halt the core before releasing the memory
  if (hb) {
    hb.reset();
    internal::startUpHeartBeatFork->killRetainer();
  }

  if (addrptr != nullptr) {
    auto ret = munmap(addrptr, length);
    if (ret == -1) {
      throw std::runtime_error("Could not unmap the memory region (" +
                               std::to_string(errno) +
                               "): " + std::string(std::strerror(errno)));
    }
  }
}

void PhysicallyLockedBuffer::startHeartbeat(size_t offset) {
  if (offset > length || offset + sizeof(uint64_t) > length) {
    throw std::runtime_error(
        "Illegal to start the heartbeat outside of the memory region");
  }

  if (!locked) {
    throw std::runtime_error(
        "Cannot start the RT core in a non-locker memory region");
  }

  // Use the global object
  internal::startUpHeartBeatFork->startMemoryRetainer(memfdLocation.str());

  auto *vaddr = reinterpret_cast<uint8_t *>(addrptr) + offset;
  std::cout << "Starting the RT core on vaddr " << uintptr_t(vaddr)
            << std::endl;
  hb = std::make_unique<RealTimeHeartbeat>(vaddr);
}
}  // namespace dory::memory
