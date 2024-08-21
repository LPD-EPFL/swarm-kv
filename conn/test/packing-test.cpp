#include <gtest/gtest.h>
#include <limits>
#include <map>
#include <vector>

#include <dory/conn/message-identifier.hpp>

class NamedKind : public dory::conn::BaseKind<NamedKind, uint64_t> {
 public:
  enum Value : uint64_t {
    Apple,
    Pear,
    Banana,
    Strawberry,
    MAX_KIND_VALUE__ = 3
  };

  constexpr NamedKind(Value v) { value = v; }

  constexpr char const *toStr() const {
    switch (value) {
      case Apple:
        return "NamedKind::Apple";
      case Pear:
        return "NamedKind::Pear";
      case Banana:
        return "NamedKind::Banana";
      case Strawberry:
        return "NamedKind::Strawberry";
      default:
        return "Out of range";
    }
  }
};

// static_assert(std::is_convertible_v<NamedKind*,
// dory::conn::BaseKind<NamedKind, std::underlying_type_t<typename
// NamedKind::Value>>*>);

TEST(Packing, PackMessage) {
  auto packed_val = dory::conn::Packer<NamedKind, unsigned, unsigned>::pack(
      NamedKind::Banana, 172, 29);
  auto [k, pid, seq] =
      dory::conn::Packer<NamedKind, unsigned, unsigned>::unpackAll(packed_val);

  EXPECT_EQ(k, NamedKind::Banana);
  EXPECT_EQ(pid, 172);
  EXPECT_EQ(seq, 29);
}
