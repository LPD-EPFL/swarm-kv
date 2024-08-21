#pragma once

#include <cstdlib>
#include <functional>
#include <memory>
#include <stdexcept>

namespace dory {
template <class T>
struct DeleteAligned {
  void operator()(T *data) const { free(data); }
};

/* `DeleteRedirected` holds the underlying buffer of an allocation object, but
 * when the deleter is called, the allocation object (which must delete the
 * underlying buffer) instead of the underlying buffer are destroyed. As a
 * result, we erase the type of the allocation object and we export the type of
 * the underlying allocation object.
 *
 * T is the type of the underlying buffer
 * U is the type of the object that holds the underlying buffer
 */
template <class T, class U>
struct DeleteRedirected {
  U *object;

  DeleteRedirected(U *object) : object{object} {}

  void operator()(T *data) noexcept {
    if (object->ptr() != static_cast<void *>(data)) {
      throw std::runtime_error("Invalid memory destruction");
    }

    delete object;
    object = nullptr;
  }
};

template <class T>
T *allocate_aligned(size_t alignment, size_t length) {
  T *raw = reinterpret_cast<T *>(aligned_alloc(alignment, sizeof(T) * length));
  if (raw == nullptr) {
    throw std::runtime_error("Insufficient memory");
  }

  return raw;
}

template <typename T>
using deleted_unique_ptr = std::unique_ptr<T, std::function<void(T *)>>;
}  // namespace dory
