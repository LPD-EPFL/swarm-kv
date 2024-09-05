#pragma once

#include <cassert>
#include <vector>
#include <unordered_map>
#include <optional>

template <typename K, typename V>
class LRUCache {
 private:
  std::unordered_map<K, std::pair<V, size_t>> item_map;
  std::vector<std::pair<K, uint64_t>> item_clock;
  uint64_t size;
  size_t hand = 0;

 public:
  LRUCache(uint64_t const size) : size{size} {}

  void put(K const& key, V const& val) {
    if (size == 0) {
      return;
    }

    auto [it, inserted] = item_map.try_emplace(key, std::make_pair(val, hand));

    if (!inserted) {
      it->second.first = val;
      item_clock[it->second.second].second += 1;
      return;
    }

    if(size < item_map.size()) {
      while (true) {
        auto& item = item_clock.at(hand);
        if(item.second == 0) {
          item_map.erase(item.first);
          break;
        }
        item.second -= 1;
        hand = (hand + 1) % item_clock.size();
      }
      item_clock.at(hand) = std::make_pair(key, 0);
      it->second.second = hand;
    } else {
      item_clock.push_back(std::make_pair(key, 0));
    }

    hand = (hand + 1) % item_clock.size();
  }

  std::optional<V> get(const K& key) {
    if (size == 0) {
      return {};
    }
    auto it = item_map.find(key);
    if (it != item_map.end()) {
      item_clock[it->second.second].second += 1;
      return it->second.first;
    }
    return {};
  }

  uint64_t getSize() const { return size; }
};
