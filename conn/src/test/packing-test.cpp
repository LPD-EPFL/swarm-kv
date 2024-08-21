#include <gtest/gtest.h>
#include <limits>
#include <map>
#include <vector>

#include "../message-identifier.hpp"

TEST(Packing, IsConstExpr) {
  static_assert(
      dory::conn::internal::number_of_bits(std::numeric_limits<int>::max()),
      "Compile-time computation of `dory::conn:::internal::number_of_bits`");
}

TEST(Packing, numberOfBitsForPositiveInt) {
  auto x = int(79);

  EXPECT_EQ(7, dory::conn::internal::number_of_bits(x));
}

TEST(Packing, numberOfBitsForNegativeInt) {
  auto x = int(-79);

  EXPECT_EQ(sizeof(int) * 8, dory::conn::internal::number_of_bits(x));
}

TEST(Packing, numberOfBitsForPositiveUint64) {
  auto x = uint64_t(79);

  EXPECT_EQ(7, dory::conn::internal::number_of_bits(x));
}

TEST(Packing, numberOfBitsForNegativeInt64) {
  auto x = int64_t(-79);

  EXPECT_EQ(sizeof(int64_t) * 8, dory::conn::internal::number_of_bits(x));
}
