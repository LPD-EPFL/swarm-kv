#include <gtest/gtest.h>

#include <map>

#include <dory/shared/error.hpp>

using namespace dory;
using namespace error;

namespace error_types {
enum ErrorType { NoError, SmallError, BigError };

char const* error_types_str[] = {"NoError", "SmallError", "BigError"};
}  // namespace error_types

using MyMaybeError =
    MaybeError<error_types::ErrorType, error_types::error_types_str>;
using NoError = MyMaybeError::NoError;

class SmallError : public MyMaybeError {
 public:
  inline error_types::ErrorType type() override { return value; }
  static error_types::ErrorType const value = error_types::SmallError;
};

class BigError : public MyMaybeError {
 public:
  BigError(int why) : why(why) {}
  int why;
  inline error_types::ErrorType type() override { return value; }
  static error_types::ErrorType const value = error_types::BigError;
};

static_assert(std::is_same_v<BigError::NoError, NoError>);
static_assert(std::is_same_v<BigError::MaybeError, MyMaybeError>);

TEST(Errors, NoError) {
  auto no_error = std::make_unique<NoError>();
  EXPECT_EQ(error_types::NoError, no_error->type());
  EXPECT_TRUE(NoError::value == no_error->type());
  EXPECT_EQ("NoError", MyMaybeError::typeStr(no_error->type()));
}

TEST(Errors, RealErrors) {
  auto small_error = std::make_unique<SmallError>();
  auto big_error = std::make_unique<BigError>(1911);

  EXPECT_NE(small_error->type(), big_error->type());
  EXPECT_EQ(error_types::SmallError, small_error->type());
  EXPECT_TRUE(SmallError::value == small_error->type());
  EXPECT_EQ("SmallError", MyMaybeError::typeStr(small_error->type()));
  EXPECT_EQ(error_types::BigError, big_error->type());
  EXPECT_TRUE(BigError::value == big_error->type());
  EXPECT_EQ("BigError", MyMaybeError::typeStr(big_error->type()));
  EXPECT_EQ(1911, dynamic_cast<BigError*>(big_error.get())->why);
  EXPECT_FALSE(small_error->ok());
  EXPECT_FALSE(big_error->ok());
}
