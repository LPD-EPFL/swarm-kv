#pragma once
#include <memory>
#include "internal/unused.hpp"

#ifndef CLANG_TIDIER
#ifndef SPDLOG_ACTIVE_LEVEL
#error "Please define the SPDLOG_ACTIVE_LEVEL for the conan package"
#endif
#endif

#include "internal/logger.hpp"

namespace dory {
using logger = std::shared_ptr<spdlog::logger>;

/**
 * Default std out logger with log level set to `spdlog::level::info`.
 * @param prefix: string prefix to prepend on every log
 **/
logger std_out_logger(std::string const &prefix);
}  // namespace dory

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_TRACE
#define LOGGER_TRACE(...) SPDLOG_LOGGER_TRACE(__VA_ARGS__)
#define LOGGER_TRACE_ACTIVE
#else
#define LOGGER_TRACE(...) DORY_ALL_UNUSED(__VA_ARGS__)
#ifdef LOGGER_TRACE_ACTIVE
#undef LOGGER_TRACE_ACTIVE
#endif
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
#define LOGGER_DEBUG(...) SPDLOG_LOGGER_DEBUG(__VA_ARGS__)
#define LOGGER_DEBUG_ACTIVE
#else
#define LOGGER_DEBUG(...) DORY_ALL_UNUSED(__VA_ARGS__)
#ifdef LOGGER_DEBUG_ACTIVE
#undef LOGGER_DEBUG_ACTIVE
#endif
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_INFO
#define LOGGER_INFO(...) SPDLOG_LOGGER_INFO(__VA_ARGS__)
#define LOGGER_INFO_ACTIVE
#else
#define LOGGER_INFO(...) DORY_ALL_UNUSED(__VA_ARGS__)
#ifdef LOGGER_INFO_ACTIVE
#undef LOGGER_INFO_ACTIVE
#endif
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_WARN
#define LOGGER_WARN(...) SPDLOG_LOGGER_WARN(__VA_ARGS__)
#define LOGGER_WARN_ACTIVE
#else
#define LOGGER_WARN(...) DORY_ALL_UNUSED(__VA_ARGS__)
#ifdef LOGGER_WARN_ACTIVE
#undef LOGGER_WARN_ACTIVE
#endif
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_ERROR
#define LOGGER_ERROR(...) SPDLOG_LOGGER_ERROR(__VA_ARGS__)
#define LOGGER_ERROR_ACTIVE
#else
#define LOGGER_ERROR(...) DORY_ALL_UNUSED(__VA_ARGS__)
#ifdef LOGGER_ERROR_ACTIVE
#undef LOGGER_ERROR_ACTIVE
#endif
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_CRITICAL
#define LOGGER_CRITICAL(...) SPDLOG_LOGGER_CRITICAL(__VA_ARGS__)
#define LOGGER_CRITICAL_ACTIVE
#else
#define LOGGER_CRITICAL(...) DORY_ALL_UNUSED(__VA_ARGS__)
#ifdef LOGGER_CRITICAL_ACTIVE
#undef LOGGER_CRITICAL_ACTIVE
#endif
#endif

#define LOGGER_INIT(name, prefix) name(dory::std_out_logger(prefix))
#define LOGGER_DECL(x) dory::logger x
#define LOGGER_DECL_INIT(name, prefix) \
  dory::logger name = dory::std_out_logger(prefix)
