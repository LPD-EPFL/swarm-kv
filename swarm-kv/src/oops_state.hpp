#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/shared/logger.hpp>

#include "latency.hpp"
#include "layout.hpp"
#include "main.hpp"  // TODO(zyf): remove/rename/whatever
#include "lru-cache.hpp"
#include "tspointers.hpp"

using timepoint = std::chrono::steady_clock::time_point;
using duration = std::chrono::steady_clock::duration;

static constexpr uint64_t MIN_RTT = 1400;
static constexpr uint64_t TS_GRANULARITY = MIN_RTT / 3;
static constexpr uint64_t MIN_TS_PER_RTT = MIN_RTT / TS_GRANULARITY;

static constexpr uint64_t BATCH_SIZE = 100;

namespace dory {

class OopsState {
 public:
  Layout layout;
  uint64_t first_client_id;
  ProcId proc_id;
  uint64_t client_idx;

  std::vector<dory::conn::ReliableConnection*> server_connections;
  std::vector<int64_t> to_poll_from_connections;
  std::vector<struct ibv_wc> wces;

  std::shared_ptr<dory::race::IndexClient> index;

  uint64_t next_log_id = 0;
  uint64_t next_kv_id = 0;

  LRUCache<HashedKey, std::pair<uint64_t, uint64_t>> pointer_cache;

  LatencyProfiler get_profiler, update_profiler, search_profiler;
  int64_t total_cache_hit_count = 0;
  std::vector<uint64_t> unreliable_maxreg_steps_counts;
  std::vector<uint64_t> oops_data_steps_counts;

  std::optional<uint64_t> ts_time_offset;
  uint64_t max_ever_seen_ts = 0;

  struct batch_measurement {
    uint64_t end_offset;
    duration sum_latency;
    duration sum_latency_gets;
    duration sum_latency_udpates;
    duration batch_duration;
    size_t size;
    size_t gets;
    size_t updates;
  };

  bool measure_batches;
  std::vector<batch_measurement> batch_measurements;
  timepoint batch_start;
  timepoint batch_last_endtime;
  batch_measurement current_batch;

  uint64_t death_point;
  uint64_t dead_servers = 0;

  // TODO(zyf): TOFIX: LOGGER_DECL_INIT(logger, "OopsState");

  OopsState(Layout _layout, conn::RcConnectionExchanger<ProcId>& rcx,
            ProcId _proc_id, uint64_t pointer_cache_size,
            bool measure_batches = false, uint64_t death_point = UINT64_MAX,
            uint64_t iter_count = 10000000)
      : layout{_layout},
        proc_id{_proc_id},
        client_idx{proc_id - layout.firstClientId()},
        pointer_cache{pointer_cache_size},
        measure_batches{measure_batches},
        death_point{death_point} {
    index = std::make_shared<dory::race::IndexClient>(proc_id, layout.num_servers, layout.bucket_bits);
    if (measure_batches) {
      batch_measurements.reserve(1 + (iter_count - 1) / BATCH_SIZE);
    }

    server_connections.reserve(layout.num_servers);
    for (size_t i = 0; i < layout.num_servers; ++i) {
      server_connections.push_back(&(rcx.connections().at(i + 1)));
    }
    to_poll_from_connections = std::vector<int64_t>(layout.num_servers, 0);
    wces = std::vector<struct ibv_wc>(128);
  }

  uint64_t newTs() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto time = ts_time_offset ? now - ts_time_offset.value() : 0;
    if (time > death_point) {
      dead_servers = 1;
      death_point = UINT64_MAX;
    }
    auto ts = time / TS_GRANULARITY;
    if (ts < max_ever_seen_ts) {
      throw std::runtime_error("Timestamp monotonicity violation");
    }
    if (ts > TsMask) {
      throw std::runtime_error("Timestamp overflow");
    }
    return ts;
  }

  void syncClock(uint64_t tsp, bool force = false) {
    if (!force && !ts_time_offset) return;
    auto ts = extractTs(tsp);
    if (!force && ts <= max_ever_seen_ts) return;
    max_ever_seen_ts = ts;
    auto t = std::chrono::steady_clock::now().time_since_epoch().count();
    auto max_offset = t - ((ts + MIN_TS_PER_RTT) * TS_GRANULARITY);
    if (!force && ts_time_offset.value() <= max_offset) return;
    ts_time_offset = std::make_optional(max_offset);
  }

  uint64_t getNextLogId() { return next_log_id++; }

  uint64_t getNextKvId() { return next_kv_id++; }

  uint64_t extractClientIdx(uint64_t tsp) {
    return extractClientProcId(tsp) - layout.firstClientId();
  }

 private:
  void addMeasurementToBatch(timepoint endtime, duration latency) {
    current_batch.sum_latency += latency;
    if (batch_last_endtime < endtime) {
      batch_last_endtime = endtime;
    }
    ++(current_batch.size);
    if (current_batch.size >= BATCH_SIZE) {
      current_batch.end_offset =
          ts_time_offset
              ? endtime.time_since_epoch().count() - ts_time_offset.value()
              : 0;
      current_batch.batch_duration = batch_last_endtime - batch_start;
      batch_measurements.push_back(current_batch);
      batch_start = batch_last_endtime;
      current_batch = {};
    }
  }

 public:
  void addGetMeasurement(timepoint starttime, timepoint endtime) {
    auto latency = endtime - starttime;
    get_profiler.addMeasurement(latency);
    if (measure_batches) {
      current_batch.sum_latency_gets += latency;
      current_batch.gets += 1;
      addMeasurementToBatch(endtime, latency);
    }
  }

  void addUpdateMeasurement(timepoint starttime, timepoint endtime) {
    auto latency = endtime - starttime;
    update_profiler.addMeasurement(latency);
    if (measure_batches) {
      current_batch.sum_latency_udpates += latency;
      current_batch.updates += 1;
      addMeasurementToBatch(endtime, latency);
    }
  }

  void measureSearch(timepoint starttime) {
    auto search_end = std::chrono::steady_clock::now();
    search_profiler.addMeasurement(search_end - starttime);
  }

  void startMeasurements(timepoint start) {
    batch_start = start;
    batch_last_endtime = batch_start;
    current_batch = {};
  }

  void reportStats(bool detailed = false) {
    if (batch_measurements.size() > 0) {
      fmt::print("\n");
      fmt::print("################ Batch stats:\n");
      fmt::print("######## Batch mid-points:\n");
      for (auto& bm : batch_measurements) {
        fmt::print("{}ns\n", bm.end_offset - (bm.batch_duration.count()/2));
      }
      fmt::print("######## Batch intervals:\n");
      for (auto& bm : batch_measurements) {
        fmt::print("{}ns / {}ops\n", bm.batch_duration.count(), bm.size);
      }
      fmt::print("######## Batch latencies:\n");
      for (auto& bm : batch_measurements) {
        fmt::print("{}ns / {}ops\n", bm.sum_latency.count(), bm.size);
      }
      fmt::print("######## Batch GET latencies:\n");
      for (auto& bm : batch_measurements) {
        fmt::print("{}ns / {}ops\n", bm.sum_latency_gets.count(), bm.gets);
      }
      fmt::print("######## Batch UPDATE latencies:\n");
      for (auto& bm : batch_measurements) {
        fmt::print("{}ns / {}ops\n", bm.sum_latency_udpates.count(),
                   bm.updates);
      }
    }

    fmt::print("\n");
    fmt::print("################ Main stats:\n");
    fmt::print("######## SEARCH stats:\n");
    fmt::print(
        "cache-hits: {}%\n",
        search_profiler.getMeasurementCount() > 0
            ? static_cast<double>(total_cache_hit_count) * 100.0 /
                  static_cast<double>(search_profiler.getMeasurementCount())
            : 0);
    search_profiler.report(detailed);
    // fmt::print("GET count: {}\n", total_get_count);
    fmt::print("######## GET stats:\n");
    get_profiler.report(detailed);
    // fmt::print("UPDATE count: {}\n", total_update_count);
    fmt::print("######## UPDATE stats:\n");
    update_profiler.report(detailed);
    fmt::print("\n");
  }
};  // class OopsState

class BasicFuture {
 public:
  BasicFuture(OopsState& _state, uint64_t _logblock_id)
      : state{_state}, logblock_id{_logblock_id} {}
  BasicFuture(BasicFuture const&) = delete;
  BasicFuture& operator=(BasicFuture const&) = delete;
  BasicFuture(BasicFuture&&) noexcept = default;
  BasicFuture& operator=(BasicFuture&&) noexcept = default;

  uint64_t futureId() { return logblock_id; }

 protected:
  OopsState& state;
  uint64_t logblock_id;
};

}  // namespace dory