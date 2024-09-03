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
#include "oops_data_future.hpp"
#include "race/client_index.hpp"
#include "tspointers.hpp"

namespace dory {

class OopsFuture : public BasicFuture {
 private:
  enum Step {
    InsertDataAndSearch,
    InsertToIndex,
    ReadSearch,
    ReadData,
    UpdateSearch,
    UpdateData,
    Linearized,
    Done  // Finished operation
  };
  Step step = Done;
  OopsDataFuture data_future;
  dory::race::IndexClient::IndexFuture index_future;

  uint64_t kv_id;
  uint64_t log_id;
  uint64_t main_server_idx;
  uint64_t match_idx;

  // std::string* key;
  // std::string* value;
 public:
  HashedKey hkey;
 public:

  bool measuring = false;
  timepoint start;

  void prepareToWrite(std::string& key, std::string& value, uint64_t log_id) {
    auto* entry = data_future.getWriteEntry();

    if (key.size() >= state.layout.key_size) {
      throw std::runtime_error("Key too long.");
    }
    if (value.size() >= state.layout.value_size) {
      throw std::runtime_error("Value too long.");
    }

    auto* kp = state.layout.keyOf(entry->kv);
    auto* vp = state.layout.valueOf(entry->kv);
    key.copy(kp, state.layout.key_size);
    value.copy(vp, state.layout.value_size);
    memset(kp + key.size(), 0,
           state.layout.key_size - key.size());
    memset(vp + value.size(), 0,
           state.layout.value_size - value.size());

    entry->kv.in_place_tsp = makeTsp(0, log_id, state.proc_id, 0);
    state.layout.hashOfTo(entry->kv, entry->kv.chsum);
  }

 public:
  OopsFuture(OopsState& _state, size_t _logblock_id)
      : BasicFuture{_state, _logblock_id},
        data_future{state, _logblock_id},
        index_future{state.index->makeFuture(_logblock_id)} {}

  void doInsert(std::string& key, std::string& value,
                bool _measuring = false) {
    start = std::chrono::steady_clock::now();
    measuring = _measuring;
    hkey = hash(key);
    main_server_idx = *reinterpret_cast<uint64_t const*>(hkey.data()) %
                      state.layout.num_servers;
    kv_id = state.getNextKvId();
    log_id = state.getNextLogId();

    prepareToWrite(key, value, log_id);

    step = InsertDataAndSearch;
    data_future
        .forKV(kv_id, main_server_idx, std::nullopt, measuring)
        .doWrite(log_id);
    index_future.search(hkey);
  }

  void doRead(std::string& key, bool _measuring = false) {
    start = std::chrono::steady_clock::now();
    measuring = _measuring;

    hkey = hash(key);
    main_server_idx = *reinterpret_cast<uint64_t const*>(hkey.data()) %
                      state.layout.num_servers;

    auto cache_entry = state.pointer_cache.get(hkey);
    if (cache_entry) {
      kv_id = (*cache_entry).first;
      auto tsp = (*cache_entry).second;

      if (measuring) {
        state.total_cache_hit_count++;
        state.measureSearch(start);
      }

      step = ReadData;
      data_future
          .forKV(kv_id, main_server_idx, tsp, measuring)
          .doRead();
    } else {
      step = ReadSearch;
      index_future.search(hkey);
    }
  }

  void doUpdate(std::string& key, std::string& value,
                bool _measuring = false) {
    start = std::chrono::steady_clock::now();
    measuring = _measuring;

    hkey = hash(key);
    main_server_idx = *reinterpret_cast<uint64_t const*>(hkey.data()) %
                      state.layout.num_servers;
    log_id = state.getNextLogId();

    auto cache_entry = state.pointer_cache.get(hkey);
    if (cache_entry) {
      kv_id = (*cache_entry).first;
      auto tsp = (*cache_entry).second;

      if (measuring) {
        state.total_cache_hit_count++;
        state.measureSearch(start);
      }

      prepareToWrite(key, value, log_id);

      step = UpdateData;
      data_future
          .forKV(kv_id, main_server_idx, tsp, measuring)
          .doWrite(log_id);
    } else {
      step = UpdateSearch;
      index_future.search(hkey);

      prepareToWrite(key, value, log_id);
    }
  }

  bool isLinearized() const {
    return step == Step::Done || step == Step::Linearized;
  }

  bool isDone() const { return step == Step::Done; }

  bool tryStepForward() {
    bool data_progress = data_future.tryStepForward();
    bool index_progress = index_future.tryStepForward();
    if (!data_progress || !index_progress) {
      return false;
    }

    switch (step) {
      case InsertDataAndSearch:
        if (index_future.isDone()) {
          if (measuring) {
            state.measureSearch(start);
          }
          step = InsertToIndex;
          index_future.tryInsert(kv_id);
          return true;
        }
        break;
      case InsertToIndex:
        if (index_future.isDone() && data_future.isDone()) {
          // TODO(zyf) TOFIX: check that index insertion was successful
          // TODO(zyf): Should we add into cache ?
          step = Done;
          return true;
        }
        break;
      case ReadSearch:
        if (index_future.isDone()) {
          if (measuring) {
            // TODO(zyf): TOFIX: check that we read the correct KV ?
            state.measureSearch(start);
          }
          step = ReadData;
          auto& result = index_future.get();
          if (result.nb_matches == 0) {
            throw std::runtime_error("Key not found during read.");
          }
          match_idx = 0;
          kv_id = result.matches[match_idx].value();
          data_future.forKV(kv_id, main_server_idx, std::nullopt, measuring)
              .doRead();
          return true;
        }
        break;
      case ReadData:
        if (measuring && data_future.isLinearized()) {
          state.addGetMeasurement(start,
                                  data_future.linearization_time.value());
        }
        if (data_future.isDone()) {
          // TODO(zyf): TOFIX: check that we read the correct KV
          state.pointer_cache.put(hkey, {kv_id, data_future.getMyTsp().value_or(0)});
          step = Done;
        } else if (data_future.isLinearized()) {
          state.pointer_cache.put(hkey, {kv_id, data_future.getMyTsp().value_or(0) | 1});
          step = Linearized;
        }
        break;
      case UpdateSearch:
        if (index_future.isDone()) {
          if (measuring) {
            // TODO(zyf): TOFIX: check that we read the correct KV ?
            state.measureSearch(start);
          }
          step = UpdateData;
          auto& result = index_future.get();
          if (result.nb_matches == 0) {
            throw std::runtime_error("Key not found during update.");
          }
          match_idx = 0;
          kv_id = result.matches[match_idx].value();
          data_future.forKV(kv_id, main_server_idx, std::nullopt, measuring)
              .doWrite(log_id);
        }
        break;
      case UpdateData:
        if (measuring && data_future.isLinearized()) {
          state.addUpdateMeasurement(start,
                                     data_future.linearization_time.value());
        }
        if (data_future.isDone()) {
          // TODO(zyf): TOFIX: check that we read the correct KV
          state.pointer_cache.put(hkey,
                                  {kv_id, data_future.getMyTsp().value_or(0)});
          step = Done;
        } else if (data_future.isLinearized()) {
          state.pointer_cache.put(
              hkey, {kv_id, data_future.getMyTsp().value_or(0) | 1});
          step = Linearized;
        }
        break;
      case Linearized:
        if (data_future.isDone()) {
          step = Done;
        }
        break;
      case Done:
        break;
      default:
        throw std::runtime_error(
            "Illegal step in OopsFuture. (tryStepForward)");
    }
    return true;
  }  // void tryStepForward()

  void addToOngoingRDMA(uint64_t server_idx, uint64_t n) {
    data_future.addToOngoingRDMA(server_idx, n);
  }
};  // class OopsFuture

}  // namespace dory
