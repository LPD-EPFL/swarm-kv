#pragma once

#include <cstdint>

namespace dory::special {
void enable_heartbeat(int data);

namespace heartbeat {
struct KernelInfo {
  KernelInfo(char const *release, char const *version, char const *arch)
      : release{release}, version{version}, arch{arch} {}

  char const *release;
  char const *version;
  char const *arch;
};

static KernelInfo const compatible_kernels[] = {
    KernelInfo{"5.4.0-74-custom", "#83+rtcore+heartbeat+nohzfull", "x86_64"}};

std::size_t constexpr CompatibleKernelsNum =
    sizeof(compatible_kernels) / sizeof(KernelInfo);
}  // namespace heartbeat
}  // namespace dory::special
