#pragma once

#include <unordered_map>
#include <optional>

template <typename K, typename V>
class LRUCache {
 private:
  std::unordered_map<K, V> item_map;
  uint64_t size;

 public:
  LRUCache(uint64_t const size) : size{size} {}

  void put(K const& key, V const& val) {
    if (size <= item_map.size()) {
      return;
    }
    auto [it, inserted] = item_map.try_emplace(key, val);
    if (!inserted) {
      it->second = val;
      return;
    }
  }

  std::optional<V> get(const K& key) {
    if (size == 0) {
      return {};
    }
    auto it = item_map.find(key);
    if (it != item_map.end()) {
      return it->second;
    }
    return {};
  }

  uint64_t getSize() const { return size; }
};
