#pragma once

#include <variant>

#include "concepts.hpp"

namespace dory {

// Bit of boilerplate for combining lambdas
template <class... Ts>
struct Overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

template <typename T, concepts::IsVariant<T> = true>
struct match {  // NOLINT
  match(T &value) : value{value} {}

  template <typename... Lambdas>
  void operator()(Lambdas... lambdas) {
    std::visit(Overloaded{lambdas...}, value);
  }

  T &value;
};

}  // namespace dory
