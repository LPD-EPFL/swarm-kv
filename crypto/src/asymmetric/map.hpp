#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>

template <class Key, class Value>
class ThreadSafeMap {
  std::mutex m_;
  std::unordered_map<Key, Value> c_;

 public:
  std::optional<Value> get(Key const& k) {
    std::unique_lock<decltype(m_)> lock(m_);

    try {
      return {c_[k]};  // Return a copy.
    } catch (std::out_of_range const&) {
      return {};
    }
  }

  template <class Value2>
  void set(Key const& k, Value2&& v) {
    std::unique_lock<decltype(m_)> lock(m_);
    c_[k] = std::forward<Value2>(v);
  }
};
