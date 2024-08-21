#pragma once

#include "branching.hpp"

namespace dory {

/**
 * @brief Optimistically search a map by checking its front first.
 *
 * @tparam Map
 * @param map
 * @param key
 * @return Map::iterator
 */
template <typename Map>
inline typename Map::iterator optimistic_find_front(
    Map& map, typename Map::key_type const& key) {
  auto b = map.begin();
  if (likely(b != map.end() && b->first == key)) {
    return b;
  }
  return map.find(key);
}

/**
 * @brief Pessimistically search a map by checking if it is empty first.
 *
 * @tparam Map
 * @param map
 * @param key
 * @return Map::iterator
 */
template <typename Map>
inline typename Map::iterator pessimistic_find(
    Map& map, typename Map::key_type const& key) {
  auto b = map.begin();
  if (likely(b == map.end())) {
    return b;
  }
  return map.find(key);
}

}  // namespace dory
