#pragma once
#include <map>

namespace dory::error {

/**
 * @brief Template for errors.
 *
 * You'll want to declare an enum listing each error kind, an array with the str
 * representation of each error as well as a way to construct different kinds of
 * error.
 *
 * @tparam ErrorType an enum with all types of errors. It should have a NoError
 *         element.
 * @tparam error_types_str[] an array with all errors' names.
 */
template <typename ErrorType, char const* const error_types_str[]>
class MaybeError {
 public:
  static char const* typeStr(ErrorType e) { return error_types_str[e]; }

  virtual inline bool ok() { return false; }
  virtual inline ErrorType type() = 0;
  virtual inline ~MaybeError() = default;

  class NoError : public MaybeError<ErrorType, error_types_str> {
   public:
    inline bool ok() override { return true; }
    inline ErrorType type() override { return value; }
    static ErrorType const value = ErrorType::NoError;
  };
};

}  // namespace dory::error
