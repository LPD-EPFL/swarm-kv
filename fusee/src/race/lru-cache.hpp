#pragma once

#include <cassert>
#include <list>
#include <unordered_map>

template <typename K, typename V>
class LRUCache{
private:
  // std::list<std::pair<K, V>> item_list;
  // std::unordered_map<K, decltype(item_list.begin())> item_map;
  std::unordered_map<K, V> item_map;
  size_t size;

  void clean() {
    // while(item_map.size() > size){
    //   auto last_it = item_list.end(); last_it --;
    //   item_map.erase(last_it->first);
    //   item_list.pop_back();
    // }
  }
public:
  LRUCache(size_t const size) : size{size} { }

  void put(K const& key, V const& val) {
    if (size == 0) {
      return;
    }
    // auto it = item_map.find(key);
    // if(it != item_map.end()){
    //   item_list.erase(it->second);
    //   item_map.erase(it);
    // }
    // item_list.push_front(std::make_pair(key,val));
    // item_map.insert(std::make_pair(key, item_list.begin()));
    // clean();
    item_map.insert_or_assign(key, val);
  }

  // bool exist(const K &key) const {
  //   return item_map.count(key) > 0;
  // }

  decltype(item_map.begin()) get(const K &key) {
    if (size == 0) {
      return item_map.end();
    }
    auto it = item_map.find(key);
    // if (it != item_map.end()) {
    //   item_list.splice(item_list.begin(), item_list, it->second);
    // }
    return it;
  }

  size_t getSize() const {
    return size;
  }

  decltype(item_map.begin()) end() {
    return item_map.end();
  }
};