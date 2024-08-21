#include <gtest/gtest.h>

#include <dory/shared/logger.hpp>

#include "impl.hpp"

/*
 * Concatenate preprocessor tokens A and B without expanding macro definitions
 * (however, if invoked from a macro, macro arguments are expanded).
 */
#define PPCAT_NX(A, B) A##B

/*
 * Concatenate preprocessor tokens A and B after macro-expanding them.
 */
#define PPCAT(A, B) PPCAT_NX(A, B)

TEST(Crypto, PPCAT(Sign, IMPL)) {
  crypto_impl::init();
  crypto_impl::publish_pub_key_nostore("p1-pk");

  unsigned char sig[crypto_impl::SignatureLength];

  char msg[] = "HELLO WORLD";
  uint64_t msg_len = 12;

  crypto_impl::sign(sig, reinterpret_cast<unsigned char*>(msg), msg_len);
}

TEST(Crypto, PPCAT(Verify, IMPL)) {
  crypto_impl::init();
  crypto_impl::publish_pub_key_nostore("p1-pk");

  unsigned char sig[crypto_impl::SignatureLength];

  char msg[] = "HELLO WORLD";
  uint64_t msg_len = 12;

  auto pk = crypto_impl::get_public_key_nostore("p1-pk");

  crypto_impl::sign(sig, reinterpret_cast<unsigned char*>(msg), msg_len);
  int ret = crypto_impl::verify(const_cast<unsigned char const*>(sig),
                                reinterpret_cast<unsigned char const*>(msg),
                                msg_len, pk);

  EXPECT_EQ(ret, 1);
}
