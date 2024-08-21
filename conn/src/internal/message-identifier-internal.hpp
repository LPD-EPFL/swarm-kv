#pragma once

#include <cstdint>

#include <dory/shared/concepts.hpp>

namespace dory::conn::internal {

template <typename NK, uint64_t V>
struct MessageKind {
  using KindValue = typename NK::Value;

  static_assert(std::is_same_v<std::underlying_type_t<KindValue>, uint64_t>);
  static int const count = KindValue::MAX_KIND_VALUE__;
  static NK constexpr Kind = NK(V);

  static char const* name() { return Kind.toStr(); }
};

template <typename T, concepts::IsIntegral<T> = true>
static constexpr unsigned number_of_bits(T x) {
  return x >= static_cast<T>(0) && x <= static_cast<T>(1)
             ? static_cast<unsigned>(x)
             : 1 + number_of_bits(
                       static_cast<typename std::make_unsigned<T>::type>(x) >>
                       1);
}

static constexpr uint64_t consecutive_ones(int x) {
  return (uint64_t(1) << x) - 1;
}

}  // namespace dory::conn::internal
