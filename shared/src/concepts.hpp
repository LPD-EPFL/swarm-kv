#pragma once

#include <iterator>
#include <type_traits>
#include <variant>

/**
 * @brief Emulation of C++20 concepts using sfinae.
 *
 * `typename = std::enable_if_t<std::is_unsigned_v<T>>`
 * becomes
 * `concepts::IsUnsigned<T> = true`
 */

namespace dory::concepts {
template <typename T>
using IsUnsigned = std::enable_if_t<std::is_unsigned_v<T>, bool>;

template <typename T>
using IsIntegral = std::enable_if_t<std::is_integral_v<T>, bool>;

template <typename T, typename U>
using IsAssignable = std::enable_if_t<std::is_assignable_v<T, U>, bool>;

// std::underlying_type is not SFINAE-friendly and should only be used on enums.
// https://stackoverflow.com/questions/49774185/how-can-i-specialize-a-class-for-enums-of-underlying-type-int
template <typename T, typename = void>
struct UnderlyingType {};

template <typename T>
struct UnderlyingType<T, std::enable_if<std::is_enum_v<T>, bool>> {
  using type = typename std::underlying_type_t<T>;
};

template <typename T>
using underlying_type_v = typename UnderlyingType<T>::type;

template <typename T, typename U>
using IsUnderlyingType =
    std::enable_if_t<std::is_same_v<underlying_type_v<T>, U>, bool>;

template <typename T>
using IsRandomIterator = std::enable_if_t<
    std::is_base_of_v<std::random_access_iterator_tag,
                      typename std::iterator_traits<T>::iterator_category>,
    bool>;

template <typename T>
struct IsVariantHelper : std::false_type {};
template <typename... Args>
struct IsVariantHelper<std::variant<Args...>> : std::true_type {};
template <typename T>
using IsVariant = std::enable_if_t<IsVariantHelper<T>::value, bool>;

template <typename T>
using IsTrivial = std::enable_if_t<std::is_trivial_v<T>, bool>;

}  // namespace dory::concepts
