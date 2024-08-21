#pragma once

/**
 * @brief Can be put inside a class to detect whether it has been moved or not
 *        without implementing custom move semantics on the object. This can
 *        then be checked within the destructor to check if handles should be
 *        released.
 *
 * Another way would be to use std::shared_ptr, but they only work for pointer-
 * like types.
 * This could eventally be replaced by unique_val.
 *
 */
class MoveIndicator {
 public:
  MoveIndicator() = default;
  MoveIndicator &operator=(MoveIndicator const &) = delete;
  MoveIndicator(MoveIndicator const &) = delete;
  MoveIndicator(MoveIndicator &&o) noexcept { o.moved = true; }
  MoveIndicator &operator=(MoveIndicator &&o) noexcept {
    if (this != &o) {
      o.moved = true;
    }
    return *this;
  }

  operator bool() const { return moved; }

 private:
  bool moved = false;
};
