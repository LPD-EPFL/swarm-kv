#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>

// Code below is taken from https://github.com/facebook/hhvm
#define always_assert(e) assert_impl(e, assert_fail_impl(e, ""))

#define assert_impl(cond, fail) \
  ((cond) ? static_cast<void>(0) : ((fail), static_cast<void>(0)))

#if defined(__GNUC__) || defined(__clang__)
#define assert_fail_impl(e, msg) \
  dory::assert_fail(#e, __FILE__, __LINE__, __PRETTY_FUNCTION__, msg)
#else
#define assert_fail_impl(e, msg) \
  dory::assert_fail(#e, __FILE__, __LINE__, __func__, msg)
#endif

namespace dory {
/* Our assert() macro uses a function that ends up in a library we don't want
 * to link this against, so provide our own simple implementation of
 * assert_fail. */
static inline void assert_fail(char const* e, char const* file,
                               unsigned int line, char const* func,
                               std::string const& /*unused*/) {
  fprintf(stderr, "%s:%u: %s: assertion `%s' failed.", file, line, func, e);
  std::abort();
}
}  // namespace dory
