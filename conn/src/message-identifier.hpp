#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <tuple>

#include <dory/shared/concepts.hpp>

#include "internal/message-identifier-internal.hpp"

namespace dory::conn {

// Identifiers
template <typename C, typename T = typename C::value_type>
static T max_id(C const& ids) {
  auto min_max_remote = std::minmax_element(ids.begin(), ids.end());
  return *min_max_remote.second;
}

template <typename C, typename T = typename C::value_type>
static T max_id(T const& id, C const& ids) {
  return std::max(id, max_id(ids));
}
}  // namespace dory::conn

namespace dory::conn {
/**
 * @brief Encapsulates constructor and comparator for Kinds.
 *
 * @tparam Integral the underlying Kind representation
 */
template <class DerivedClass, typename Integral>
class BaseKind {
 public:
  // Couldn't get this to automaticaly produce a constructor.
  // template<typename T, concepts::IsUnderlyingType<T, Integral> = true>
  // constexpr BaseKind(T value) : value{static_cast<Integral>(value)} { }

  constexpr bool operator==(BaseKind a) const { return value == a.value; }
  constexpr bool operator!=(BaseKind a) const { return value != a.value; }

  constexpr bool operator==(Integral a) const { return value == a; }
  constexpr bool operator!=(Integral a) const { return value != a; }

  constexpr Integral operator<<(int n) const { return value << n; }
  constexpr Integral operator>>(int n) const { return value >> n; }

  constexpr bool operator<(BaseKind a) const { return value < a.value; }

  constexpr Integral integral() const { return value; }

  // Note: We do not set the real value as this is directly done within the
  //       derived class. We still initialize it to 0 for the constructor to be
  //       constexpr.
  Integral value = 0;
};
}  // namespace dory::conn

namespace dory::conn {
template <typename Kind, typename ProcId, typename ReqId>
class Packer {
  // Kind should be based on BaseKind to be valid.
  // note: BaseKind uses CRTP, hence the `strange` seconde type.
  static_assert(std::is_convertible_v<
                Kind*,
                BaseKind<Kind, std::underlying_type_t<typename Kind::Value>>*>);

 public:
  using KindType = Kind;
  using ProcIdType = ProcId;
  using ReqIdType = ReqId;

  static_assert(std::is_unsigned_v<ProcId> && std::is_unsigned_v<ReqId>);

  static inline constexpr uint64_t pack(Kind k, ProcId pid, ReqId seq) {
    return (k << KindShift) | (static_cast<uint64_t>(pid) << PidShift) | seq;
  }

  static inline constexpr ProcId unpackPid(uint64_t packed) {
    return static_cast<ProcId>((packed & PidMask) >> PidShift);
  }

  static inline constexpr Kind unpackKind(uint64_t packed) {
    return static_cast<Kind>(
        static_cast<typename Kind::Value>(packed >> KindShift));
  }

  static inline constexpr ReqId unpackReq(uint64_t packed) {
    return static_cast<ReqId>(packed & ReqMask);
  }

  static inline std::tuple<Kind, ProcId, ReqId> unpackAll(uint64_t packed) {
    return std::make_tuple(unpackKind(packed), unpackPid(packed),
                           unpackReq(packed));
  }

 private:
  static int constexpr KindSize =
      internal::number_of_bits(internal::MessageKind<Kind, 0>::count);
  static int constexpr PidSize = sizeof(uint16_t) * 8 - KindSize;
  static int constexpr KindShift = sizeof(Kind) * 8 - KindSize;
  static int constexpr PidShift = KindShift - PidSize;
  static int constexpr ReqEraseShift = KindSize + PidSize;
  static uint64_t constexpr PidMask = internal::consecutive_ones(PidSize)
                                      << PidShift;
  static uint64_t constexpr ReqMask =
      internal::consecutive_ones(sizeof(uint64_t) * 8 - KindSize - PidSize);
};
}  // namespace dory::conn
