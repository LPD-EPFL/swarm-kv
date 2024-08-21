#pragma once

#include "internal/queue/readerwriterqueue.h"

namespace dory::third_party::sync {

/**
 * @brief Single producer single consumer lock-free queue.
 *
 * @tparam T produced/consumed type.
 */
template <typename T>
using SpscQueue = moodycamel::ReaderWriterQueue<T>;

/**
 * @brief Single producer single consumer lock-free queue which supports
 *        blocking operations.
 *
 * @tparam T produced/consumed type.
 */
template <typename T>
using BlockingSpscQueue = moodycamel::BlockingReaderWriterQueue<T>;

}  // namespace dory::third_party::sync
