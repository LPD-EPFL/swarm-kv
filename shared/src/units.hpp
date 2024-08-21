#pragma once

#include <cmath>
#include <iomanip>
#include <iostream>

namespace dory::units {
// Byte
template <typename T>
static constexpr size_t bytes(T b) noexcept {
  return static_cast<size_t>(b);
}

static constexpr size_t operator"" _B(unsigned long long x) noexcept {
  return bytes(x);
}

// KibiByte
static size_t constexpr KiB = 1024_B;

template <typename T>
static constexpr size_t kibibytes(T kib) noexcept {
  return static_cast<size_t>(kib * KiB);
}

static constexpr size_t kibibytes(long double kib) noexcept {
  return static_cast<size_t>(std::llround(kib * static_cast<long double>(KiB)));
}

static constexpr size_t operator"" _KiB(unsigned long long x) noexcept {
  return kibibytes(x);
}

static constexpr size_t operator"" _KiB(long double x) noexcept {
  return kibibytes(x);
}

// MebiByte
static size_t constexpr MiB = 1024_KiB;

template <typename T>
static constexpr size_t mebibytes(T mib) noexcept {
  return static_cast<size_t>(mib * MiB);
}

static constexpr size_t mebibytes(long double mib) noexcept {
  return static_cast<size_t>(std::llround(mib * static_cast<long double>(MiB)));
}

static constexpr size_t operator"" _MiB(unsigned long long x) noexcept {
  return mebibytes(x);
}

static constexpr size_t operator"" _MiB(long double x) noexcept {
  return mebibytes(x);
}

// GibiByte
static size_t constexpr GiB = 1024_MiB;

template <typename T>
static constexpr size_t gibibytes(T gib) noexcept {
  return static_cast<size_t>(gib * GiB);
}

static constexpr size_t gibibytes(long double gib) noexcept {
  return static_cast<size_t>(std::llround(gib * static_cast<long double>(GiB)));
}

static constexpr size_t operator"" _GiB(unsigned long long x) noexcept {
  return gibibytes(x);
}

static constexpr size_t operator"" _GiB(long double x) noexcept {
  return gibibytes(x);
}
}  // namespace dory::units
