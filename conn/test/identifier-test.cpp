#include <gtest/gtest.h>
#include <vector>

#include <dory/conn/message-identifier.hpp>

TEST(CollectionOfIdentifiers, MaxFromIntVector) {
  std::vector<int> v{5, 1, 9, 13, 7};
  auto max_id = dory::conn::max_id(v);

  EXPECT_EQ(13, max_id);
}

TEST(CollectionOfIdentifiers, MaxFromUint64Vector) {
  std::vector<uint64_t> v{5, 26, 9, 13, 7};
  auto max_id = dory::conn::max_id(v);

  EXPECT_EQ(26, max_id);
}

TEST(CollectionOfIdentifiers, MaxFromIntSet) {
  std::set<int> s{5, 1, 9, 13, 7};
  auto max_id = dory::conn::max_id(s);

  EXPECT_EQ(13, max_id);
}

TEST(CollectionOfIdentifiers, MaxFromUint64Set) {
  std::set<uint64_t> s{5, 26, 9, 13, 7};
  auto max_id = dory::conn::max_id(s);

  EXPECT_EQ(26, max_id);
}

TEST(CollectionOfIdentifiersWithExtra, MaxFromIntVector) {
  std::vector<int> v{5, 1, 9, 13, 7};
  auto max_id = dory::conn::max_id(95, v);

  EXPECT_EQ(95, max_id);
}

TEST(CollectionOfIdentifiersWithExtra, MaxFromUint64Vector) {
  std::vector<uint64_t> v{5, 26, 9, 13, 7};
  auto max_id = dory::conn::max_id(uint64_t(10), v);

  EXPECT_EQ(26, max_id);
}

TEST(CollectionOfIdentifiersWithExtra, MaxFromIntSet) {
  std::set<int> s{5, 1, 9, 13, 7};
  auto max_id = dory::conn::max_id(95, s);

  EXPECT_EQ(95, max_id);
}

TEST(CollectionOfIdentifiersWithExtra, MaxFromUint64Set) {
  std::set<uint64_t> s{5, 26, 9, 13, 7};
  auto max_id = dory::conn::max_id(uint64_t(10), s);

  EXPECT_EQ(26, max_id);
}
