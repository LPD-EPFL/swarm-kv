#include <list>
#include <unordered_map>
#include <assert.h>

template <typename K, typename V>
class LRUCache{
private:
  std::list<std::pair<K, V>> item_list;
  std::unordered_map<K, decltype(item_list.begin())> item_map;
  size_t size;

  void clean() {
    while(item_map.size() > size){
      auto last_it = item_list.end(); last_it --;
      item_map.erase(last_it->first);
      item_list.pop_back();
    }
  };
public:
  LRUCache(size_t const size) : size(size) { }

  void put(K const& key, V const& val) {
    auto it = item_map.find(key);
    if(it != item_map.end()){
      item_list.erase(it->second);
      item_map.erase(it);
    }
    item_list.push_front(std::make_pair(key,val));
    item_map.insert(std::make_pair(key, item_list.begin()));
    clean();
  }

  bool exist(const KEY_T &key) const {
    return item_map.count(key) > 0;
  }

  V& get(const K &key) const {
    assert(exist(key));
    auto it = item_map.find(key);
    item_list.splice(item_list.begin(), item_list, it->second);
    return it->second->second;
  }
};