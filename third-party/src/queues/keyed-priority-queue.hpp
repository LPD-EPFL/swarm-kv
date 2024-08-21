#pragma once

#include "internal/updatable_priority_queue.h"

namespace dory::third_party::queues {

/**
 * @brief A priority queue in which each element has a key and an updatable
 * value.
 *
 * @tparam Key must be convertible to int
 * @tparam Value must support int addition/subtraction
 */
template <typename Key, typename Value>
class KeyedPriorityQueue {
 public:
  /**
   * @brief Return the key corresponding to the highest element in the queue.
   *
   * @return Key const&
   */
  inline Key const& top() { return priority_queue.top().key; }

  /**
   * @brief Increment an element in the queue. Inserts the element if it does
   * not exist.
   *
   * @param key
   */
  inline void increment(Key const& key) { priority_queue.increment(key); }

  /**
   * @brief Decrement an element in the queue. Inserts the element if it does
   * not exist.
   *
   * @param key
   */
  inline void decrement(Key const& key) { priority_queue.decrement(key); }

  inline void set(Key const& key, Value const& value) {
    priority_queue.set(key, value);
  }

 private:
  better_priority_queue::updatable_priority_queue<Key, Value> priority_queue;
};

}  // namespace dory::third_party::queues
