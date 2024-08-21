#include "pinning.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>

// Workaround glibc bug
#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

namespace dory {
void pin_main_to_core(int cpu_id) {
  auto pid = getpid();
  auto tid = gettid();

  if (pid != tid) {
    throw std::runtime_error("pin_main_to_core not called from main thread");
  }

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);

  auto set_result = sched_setaffinity(pid, sizeof(cpu_set_t), &cpuset);
  if (set_result != 0) {
    throw std::runtime_error("Error calling sched_setaffinity: " +
                             std::string(std::strerror(errno)));
  }
}

void reset_main_pinning() {
  auto pid = getpid();
  auto tid = gettid();

  if (pid != tid) {
    throw std::runtime_error("pin_main_to_core not called from main thread");
  }

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);

  for (int cpu_id = 0; cpu_id < CPU_SETSIZE; cpu_id++) {
    CPU_SET(cpu_id, &cpuset);
  }

  auto set_result = sched_setaffinity(pid, sizeof(cpu_set_t), &cpuset);
  if (set_result != 0) {
    throw std::runtime_error("Error calling sched_setaffinity: " +
                             std::string(std::strerror(errno)));
  }
}

void pin_thread_to_core(std::thread &thd, int cpu_id) {
  // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
  // only CPU i as set.
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);
  int rc =
      pthread_setaffinity_np(thd.native_handle(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    throw std::runtime_error("Error calling pthread_setaffinity_np: " +
                             std::string(std::strerror(rc)));
  }
}

void reset_thread_pinning(std::thread &thd) {
  // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
  // only CPU i as set.
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);

  for (int cpu_id = 0; cpu_id < CPU_SETSIZE; cpu_id++) {
    CPU_SET(cpu_id, &cpuset);
  }

  int rc =
      pthread_setaffinity_np(thd.native_handle(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    throw std::runtime_error("Error calling pthread_setaffinity_np: " +
                             std::string(std::strerror(rc)));
  }
}

void set_thread_name(std::thread::native_handle_type pthread,
                     char const *name) {
  if (strlen(name) > 15) {
    throw std::runtime_error("Thread names must be at most 15 chars long.");
  }

  int rc = pthread_setname_np(pthread, name);

  if (rc != 0) {
    throw std::runtime_error("Error calling pthread_setname_np: " +
                             std::string(std::strerror(rc)));
  }
}

void set_thread_name(std::thread &thd, char const *name) {
  set_thread_name(thd.native_handle(), name);
}
}  // namespace dory
