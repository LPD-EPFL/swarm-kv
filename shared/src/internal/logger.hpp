#pragma once

// __PRETTY_FUNCTION__ avoids this problem:
// https://clang.llvm.org/extra/clang-tidy/checks/bugprone-lambda-function-name.html
#ifndef SPDLOG_FUNCTION
#if defined(__GNUC__) || defined(__clang__)
#define SPDLOG_FUNCTION static_cast<const char *>(__PRETTY_FUNCTION__)
#endif
#endif

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
