#pragma once

#include <climits>

namespace dory {

// https://stackoverflow.com/questions/1392059/algorithm-to-generate-bit-mask

template <typename R>
static constexpr R bitmask(unsigned int const onecount) {
  //  return (onecount != 0)
  //      ? (static_cast<R>(-1) >> ((sizeof(R) * CHAR_BIT) - onecount))
  //      : 0;
  return static_cast<R>(-(onecount != 0)) &
         (static_cast<R>(-1) >> ((sizeof(R) * CHAR_BIT) - onecount));
}

}  // namespace dory
