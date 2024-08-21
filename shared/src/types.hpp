#pragma once

#include <functional>
#include <optional>

namespace dory {

// We often want to represent a ptrdiff that is positive, but ptrdiff_t has no
// unsigned counterpart. The *de facto* one is size_t. But We would like to
// tell the reader about what we are actually representing, hence the alias
// for uptrdiff_t.
using uptrdiff_t = size_t;

// Using optionals is better than having dummy values or dangling pointers.
template <typename T>
using Delayed = std::optional<T>;
template <typename T>
using DelayedRef = std::optional<std::reference_wrapper<T>>;

}  // namespace dory
