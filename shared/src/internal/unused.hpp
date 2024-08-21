#pragma once

// Inspired by https://stackoverflow.com/a/31470425

#define DORY_UNUSED0()

#define DORY_UNUSED1(x0) (void)(sizeof(x0))

#define DORY_UNUSED2(x0, x1) (void)(sizeof(x0)), DORY_UNUSED1(x1)

#define DORY_UNUSED3(x0, x1, x2) (void)(sizeof(x0)), DORY_UNUSED2(x1, x2)

#define DORY_UNUSED4(x0, x1, x2, x3) \
  (void)(sizeof(x0)), DORY_UNUSED3(x1, x2, x3)

#define DORY_UNUSED5(x0, x1, x2, x3, x4) \
  (void)(sizeof(x0)), DORY_UNUSED4(x1, x2, x3, x4)

#define DORY_UNUSED6(x0, x1, x2, x3, x4, x5) \
  (void)(sizeof(x0)), DORY_UNUSED5(x1, x2, x3, x4, x5)

#define DORY_UNUSED7(x0, x1, x2, x3, x4, x5, x6) \
  (void)(sizeof(x0)), DORY_UNUSED6(x1, x2, x3, x4, x5, x6)

#define DORY_UNUSED8(x0, x1, x2, x3, x4, x5, x6, x7) \
  (void)(sizeof(x0)), DORY_UNUSED7(x1, x2, x3, x4, x5, x6, x7)

#define DORY_UNUSED9(x0, x1, x2, x3, x4, x5, x6, x7, x8) \
  (void)(sizeof(x0)), DORY_UNUSED8(x1, x2, x3, x4, x5, x6, x7, x8)

#define DORY_UNUSED10(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9) \
  (void)(sizeof(x0)), DORY_UNUSED9(x1, x2, x3, x4, x5, x6, x7, x8, x9)

#define DORY_UNUSED11(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) \
  (void)(sizeof(x0)), DORY_UNUSED10(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10)

#define DORY_UNUSED12(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11) \
  (void)(sizeof(x0)),                                                   \
      DORY_UNUSED11(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11)

#define DORY_UNUSED13(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12) \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED12(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12)

#define DORY_UNUSED14(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13)                                                   \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED13(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)

#define DORY_UNUSED15(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14)                                              \
  (void)(sizeof(x0)), DORY_UNUSED14(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, \
                                    x11, x12, x13, x14)

#define DORY_UNUSED16(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15)                                         \
  (void)(sizeof(x0)), DORY_UNUSED15(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, \
                                    x11, x12, x13, x14, x15)

#define DORY_UNUSED17(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16)                                    \
  (void)(sizeof(x0)), DORY_UNUSED16(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, \
                                    x11, x12, x13, x14, x15, x16)

#define DORY_UNUSED18(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17)                               \
  (void)(sizeof(x0)), DORY_UNUSED17(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, \
                                    x11, x12, x13, x14, x15, x16, x17)

#define DORY_UNUSED19(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18)                          \
  (void)(sizeof(x0)), DORY_UNUSED18(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, \
                                    x11, x12, x13, x14, x15, x16, x17, x18)

#define DORY_UNUSED20(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19)                     \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED19(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19)

#define DORY_UNUSED21(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20)                \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED20(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20)

#define DORY_UNUSED22(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21)           \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED21(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21)

#define DORY_UNUSED23(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22)      \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED22(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21, x22)

#define DORY_UNUSED24(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23) \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED23(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21, x22, x23)

#define DORY_UNUSED25(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, \
                      x24)                                                   \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED24(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, x24)

#define DORY_UNUSED26(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, \
                      x24, x25)                                              \
  (void)(sizeof(x0)), DORY_UNUSED25(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, \
                                    x11, x12, x13, x14, x15, x16, x17, x18,  \
                                    x19, x20, x21, x22, x23, x24, x25)

#define DORY_UNUSED27(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, \
                      x24, x25, x26)                                         \
  (void)(sizeof(x0)), DORY_UNUSED26(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, \
                                    x11, x12, x13, x14, x15, x16, x17, x18,  \
                                    x19, x20, x21, x22, x23, x24, x25, x26)

#define DORY_UNUSED28(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, \
                      x24, x25, x26, x27)                                    \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED27(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, x24,   \
                    x25, x26, x27)

#define DORY_UNUSED29(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, \
                      x24, x25, x26, x27, x28)                               \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED28(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, x24,   \
                    x25, x26, x27, x28)

#define DORY_UNUSED30(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, \
                      x24, x25, x26, x27, x28, x29)                          \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED29(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, x24,   \
                    x25, x26, x27, x28, x29)

#define DORY_UNUSED31(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, \
                      x13, x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, \
                      x24, x25, x26, x27, x28, x29, x30)                     \
  (void)(sizeof(x0)),                                                        \
      DORY_UNUSED30(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13,  \
                    x14, x15, x16, x17, x18, x19, x20, x21, x22, x23, x24,   \
                    x25, x26, x27, x28, x29, x30)

#define VA_NUM_ARGS_IMPL(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11,    \
                         _12, _13, _14, _15, _16, _17, _18, _19, _20, _21,    \
                         _22, _23, _24, _25, _26, _27, _28, _29, _30, _31, N, \
                         ...)                                                 \
  N

#define VA_NUM_ARGS(...)                                                       \
  VA_NUM_ARGS_IMPL(100, ##__VA_ARGS__, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, \
                   21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, \
                   5, 4, 3, 2, 1, 0)

#define DORY_ALL_UNUSED_IMPL_(nargs) DORY_UNUSED##nargs
#define DORY_ALL_UNUSED_IMPL(nargs) DORY_ALL_UNUSED_IMPL_(nargs)
#define DORY_ALL_UNUSED(...) \
  DORY_ALL_UNUSED_IMPL(VA_NUM_ARGS(__VA_ARGS__))(__VA_ARGS__)
