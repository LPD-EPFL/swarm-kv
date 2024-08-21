#pragma once

#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <malloc.h>
#include <time.h>

#define K_128 131072
#define K_128_ 131071

#define K_256 262144
#define K_256_ 262143

#define K_512 524288
#define K_512_ 524287

#define M_1 1048576
#define M_1_ 1048575

#define M_2 2097152
#define M_2_ 2097151

#define M_4 4194304
#define M_4_ 4194303

#define M_8 8388608
#define M_8_ 8388607

#define M_16 16777216
#define M_16_ 16777215

#define M_32 33554432
#define M_32_ 33554431

#define M_128 134217728
#define M_128_ 134217727

#define M_256 268435456
#define M_256_ 268435455

#define M_512 536870912
#define M_512_ 536870911

#define M_1024 1073741824
#define M_1024_ 1073741823

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/* Utility functions */
static inline uint32_t hrd_fastrand(uint64_t* seed) {
  *seed = *seed * 1103515245 + 12345;
  return (uint32_t)(*seed >> 32);
}

/* Like printf, but red. Limited to 1000 characters. */
static void hrd_red_printf(const char* format, ...) {
#define RED_LIM 1000
  va_list args;
  int i;

  char buf1[RED_LIM], buf2[2*RED_LIM];
  memset(buf1, 0, RED_LIM);
  memset(buf2, 0, RED_LIM);

  va_start(args, format);

  /* Marshal the stuff to print in a buffer */
  vsnprintf(buf1, RED_LIM, format, args);

  /* Probably a bad check for buffer overflow */
  for (i = RED_LIM - 1; i >= RED_LIM - 50; i--) {
    assert(buf1[i] == 0);
  }

  /* Add markers for red color and reset color */
  snprintf(buf2, 2000, "\033[31m%s\033[0m", buf1);

  /* Probably another bad check for buffer overflow */
  for (i = RED_LIM - 1; i >= RED_LIM - 50; i--) {
    assert(buf2[i] == 0);
  }

  printf("%s", buf2);

  va_end(args);
}
