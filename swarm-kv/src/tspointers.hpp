#pragma once

#include <cstdint>

constexpr uint64_t TsSize = 32;
constexpr uint64_t LogIdSize = 24;
constexpr uint64_t ClientIdSize = 7;

constexpr uint64_t TsMask = (1ULL << TsSize) - 1;
constexpr uint64_t LogIdMask = (1ULL << LogIdSize) - 1;
constexpr uint64_t ClientProcIdMask = (1ULL << ClientIdSize) - 1;

constexpr uint64_t TsOffset = 64 - TsSize;
constexpr uint64_t LogIdOffset = TsOffset - LogIdSize;
constexpr uint64_t ClientProcIdOffset = LogIdOffset - ClientIdSize;

static_assert(TsSize + LogIdSize + ClientIdSize + 1 == 64);
static_assert(ClientProcIdOffset == 1);

static uint64_t makeTsp(uint64_t ts, uint64_t log_id, uint64_t client_proc_id,
                        bool validated) {
  if(ts > TsMask) {
    throw std::runtime_error("Timestamp overflow in tsp.");
  }
  if(log_id > LogIdMask) {
    throw std::runtime_error("Log id overflow in tsp.");
  }
  if(client_proc_id > ClientProcIdMask) {
    throw std::runtime_error("Client id overflow in tsp.");
  }
  uint64_t out = (ts & TsMask) << TsOffset;
  out |= (log_id & LogIdMask) << LogIdOffset;
  out |= (client_proc_id & ClientProcIdMask) << ClientProcIdOffset;
  out |= (validated ? 1 : 0);
  return out;
}

static uint64_t extractTs(uint64_t v) { return (v >> TsOffset) & TsMask; }
static uint64_t extractLogId(uint64_t v) {
  return (v >> LogIdOffset) & LogIdMask;
}
static uint64_t extractClientProcId(uint64_t v) {
  return (v >> ClientProcIdOffset) & ClientProcIdMask;
}
static uint64_t filterLogIdAndClientProcId(uint64_t v) {
  return v & (
    (LogIdMask << LogIdOffset) |
    (ClientProcIdMask << ClientProcIdOffset)
  );
}
static bool isVerified(uint64_t v) { return (v & 1) != 0; }

// static uint64_t makeNextTs(uint64_t log_id, uint64_t client_proc_id, bool validated) {
//   auto ts = extractTs(prev);
//   // Alternative 1:
//   // auto temp = makeTsp(ts, log_id, client_proc_id, validated);
//   // if(temp <= v) temp += (1 << TsOffset);
//   // return temp;
//   // Alternative 2:
//   return makeTsp(ts + 1, log_id, client_proc_id, validated);
// }

static uint64_t validateTsp(uint64_t tsp) { return tsp | 1; }