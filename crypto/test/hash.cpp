#include <gtest/gtest.h>

#include <iomanip>
#include <sstream>
#include <string>

#include <dory/crypto/hash/blake2b.hpp>
#include <dory/crypto/hash/blake3.hpp>

TEST(Crypto, GenerateBlake2Hash) {
  dory::crypto::hash::Blake2Hash hash;
  std::string message("This is a message to be hashed");

  hash = dory::crypto::hash::blake2b(message.begin(), message.end());

  // Reference computed with https://www.toolkitbay.com/tkb/tool/BLAKE2b_256
  std::string reference_digest(
      "8701d6d4a9cb25e9a3d039e558963a4156947474b1ecab07fcd96f99e43ca965");

  std::stringstream output_digest;
  for (auto v : hash) {
    output_digest << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(v);
  }

  EXPECT_EQ(reference_digest, output_digest.str());
}

TEST(Crypto, GenerateBlake3Hash) {
  dory::crypto::hash::Blake3Hash hash;
  std::string message("This is a message to be hashed");

  hash = dory::crypto::hash::blake3(message.begin(), message.end());

  // Reference computed with https://connor4312.github.io/blake3/index.html
  // This tool (http://www.toolkitbay.com/tkb/tool/BLAKE3) also exists, but it
  // didn't work.
  std::string reference_digest(
      "3b0c0529dc163f0327c96ae57cff01d9735b04e7915f58ec5d94365d433ad17b");

  std::stringstream output_digest;
  for (auto v : hash) {
    output_digest << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(v);
  }

  EXPECT_EQ(reference_digest, output_digest.str());
}
