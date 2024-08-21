#pragma once

#include "internal/queue/blockingconcurrentqueue.h"
#include "internal/queue/concurrentqueue.h"

namespace dory::third_party::sync {

/**
 * @brief Multiple producers multiple consumer lock-free queue.
 *
 * @tparam T produced/consumed type.
 */
template <typename T>
using MpmcQueue = moodycamel::ConcurrentQueue<T>;

/**
 * @brief Multiple producers multiple consumers lock-free queue which supports
 *        blocking operations.
 *
 * @tparam T produced/consumed type.
 */
template <typename T>
using BlockingMpmcQueue = moodycamel::BlockingConcurrentQueue<T>;

/**
 * @brief A token used to fasten MPMC enqueueing.
 *
 */
using MpmcProducerToken = moodycamel::ProducerToken;
}  // namespace dory::third_party::sync
