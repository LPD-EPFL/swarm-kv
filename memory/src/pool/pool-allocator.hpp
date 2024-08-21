#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

#include <dory/shared/types.hpp>

namespace dory::memory::pool {
template <typename T>
struct Chunk {
  Chunk *next;
  T obj;
};

template <typename T>
class PoolAllocator {
 public:
  using value_type = T;

  PoolAllocator(void *buf_start, size_t buf_size, size_t max_objects,
                void *offset_reference, size_t alignment = 1)
      : max_objects{max_objects},
        offset_reference{reinterpret_cast<uintptr_t>(offset_reference)},
        alignment{alignment},
        stride{computeStride(alignment)} {
    // Compute true start accounting for alignment
    auto actual_buf_start = trueStart(buf_start);
    auto buf_start_num = reinterpret_cast<uintptr_t>(buf_start);
    if (actual_buf_start >= buf_start_num + buf_size) {
      throw std::runtime_error(
          "Not enough space to allocate after accounting for alignment!");
    }

    this->buf_start = reinterpret_cast<void *>(actual_buf_start);
    buf_next = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(buf_start) +
                                        buf_size);

    auto actual_buf_end = trueEnd(this->buf_start, max_objects, stride);
    if (actual_buf_end >= reinterpret_cast<uintptr_t>(buf_next)) {
      throw std::runtime_error("Not enough space to allocate " +
                               std::to_string(max_objects) + " objects!");
    }

    initialize();
  }

  std::pair<void *, size_t> remaining() {
    auto *remaining_buf_start =
        reinterpret_cast<uint8_t *>(trueEnd(buf_start, max_objects, stride)) +
        1;
    auto remaining_buf_size = reinterpret_cast<size_t>(buf_next) -
                              reinterpret_cast<size_t>(remaining_buf_start);

    return std::make_pair(reinterpret_cast<void *>(remaining_buf_start),
                          remaining_buf_size);
  }

  // Assumes worst possible start
  static size_t alignedSpaceRequirement(size_t alignment, size_t max_objects) {
    auto space_required = spaceRequired(alignment, max_objects);
    return space_required + alignment + HeaderSize;
  }

  // Assumes the start happens to be aligned start
  static size_t spaceRequired(size_t alignment, size_t max_objects) {
    if (max_objects == 0) {
      return 0;
    }

    auto stride = computeStride(alignment);
    return trueEnd(reinterpret_cast<void *>(0), max_objects, stride) + 1;
  }

  template <typename... Args>
  T *create(Args &&... args) {
    auto store_at = next;

    if (store_at == nullptr) {
      return nullptr;
    }

    new (&store_at->obj) T(std::forward<Args>(args)...);
    next = store_at->next;

    return &(store_at->obj);
  }

  uptrdiff_t offset(T *obj) {
    return reinterpret_cast<uintptr_t>(obj) - offset_reference;
  }

  void destroy(T *obj) {
    auto subtract = offsetof(Chunk<T>, obj);
    auto *chunk_ptr = reinterpret_cast<char *>(obj) - subtract;
    auto chunk = reinterpret_cast<Chunk<T> *>(chunk_ptr);

    obj->~T();
    // ::operator delete(&chunk->obj);

    chunk->next = next;
    next = chunk;
  }

 private:
  void initialize() {
    next = nullptr;

    if (max_objects == 0) {
      return;
    }

    auto *s = reinterpret_cast<uint8_t *>(buf_start);
    next = reinterpret_cast<Chunk<T> *>(s);

    auto last =
        reinterpret_cast<Chunk<T> *>(chunkStart(s, max_objects - 1, stride));
    last->next = nullptr;

    for (size_t i = 0; i < max_objects - 1; i++) {
      auto curr = reinterpret_cast<Chunk<T> *>(chunkStart(s, i, stride));

      auto nxt = reinterpret_cast<Chunk<T> *>(chunkStart(s, i + 1, stride));
      curr->next = nxt;
    }
  }

  uintptr_t trueStart(void *start) {
    auto s = reinterpret_cast<uintptr_t>(start) + HeaderSize;
    return roundUp(s, alignment) - HeaderSize;
  }

  static uintptr_t chunkStart(void *actual_start, size_t idx, size_t stride) {
    auto s = reinterpret_cast<uintptr_t>(actual_start);
    return s + stride * idx;
  }

  static uintptr_t trueEnd(void *actual_start, size_t max_objects,
                           size_t stride) {
    if (max_objects == 0) {
      auto s = reinterpret_cast<uintptr_t>(actual_start);
      return s;
    }

    return chunkStart(actual_start, max_objects - 1, stride) +
           sizeof(Chunk<T>) - 1;
  }

  template <typename U>
  static typename std::enable_if<std::is_unsigned<U>::value, U>::type roundUp(
      U numToRound, size_t alignment) {
    if (!isPowerof2(alignment)) {
      throw std::runtime_error("Provided alignment is not a power of two!");
    }
    return (numToRound + static_cast<U>(alignment) - 1) &
           -static_cast<U>(alignment);
  }

  template <typename U>
  static constexpr bool isPowerof2(U v) {
    return v && ((v & (v - 1)) == 0);
  }

  static size_t computeStride(size_t alignment) {
    return roundUp(sizeof(Chunk<T>), alignment);
  }

  void *buf_start;
  void *buf_next;
  size_t max_objects;
  uintptr_t offset_reference;
  size_t alignment;
  size_t stride;

  static size_t constexpr HeaderSize = offsetof(Chunk<T>, obj);

  Chunk<T> *next;
};

class ArenaPoolAllocator {
 public:
  ArenaPoolAllocator(void *buf_start, size_t buf_size, void *offset_reference)
      : buf_start{buf_start},
        buf_next{buf_start},
        buf_size{buf_size},
        offset_reference{offset_reference} {}

  template <typename T>
  std::unique_ptr<PoolAllocator<T>> createPool(size_t max_objects,
                                               size_t alignment = 1) {
    auto obj = std::make_unique<PoolAllocator<T>>(
        buf_next, buf_size, max_objects, offset_reference, alignment);

    auto [remaining_buf_start, remaining_buf_size] = obj->remaining();
    buf_next = remaining_buf_start;
    buf_size = remaining_buf_size;

    return obj;
  }

  uptrdiff_t offset() { return reinterpret_cast<uintptr_t>(offset_reference); }

 private:
  void *buf_start;
  void *buf_next;
  size_t buf_size;
  void *offset_reference;
};
}  // namespace dory::memory::pool
