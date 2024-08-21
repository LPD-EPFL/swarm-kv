#pragma once

namespace dory {

template <typename... T>
void ignore(T&&... /*unused*/) {}

}  // namespace dory
