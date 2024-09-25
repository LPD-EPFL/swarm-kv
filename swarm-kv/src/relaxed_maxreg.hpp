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
#include "race/client_index.hpp"
#include "tspointers.hpp"
#include "unreliable_maxreg.hpp"

namespace dory {

class RelaxedMaxRegFuture : public BasicFuture {
 private:
  enum Step {
    TryRead,
    ReadLog,
    Read,
    TryWrite,
    Write,
    Linearized,
    Done,
  };
  Step step = Done;
  std::vector<UnreliableMaxRegFuture> maxregs;

  uint64_t max_seen_tsp = 0;
  LogEntry* read_entry = nullptr;

  bool measuring = false;

  void resetProgressMarkers() {
    for (auto& maxreg : maxregs) {
      maxreg.made_progress = false;
    }
  }

 public:
  RelaxedMaxRegFuture(OopsState& _state, size_t _logblock_id)
      : BasicFuture{_state, _logblock_id} {
    maxregs.reserve(state.layout.num_servers);
    for (size_t i = 0; i < state.layout.num_servers; i++) {
      maxregs.emplace_back(state, logblock_id, i);
    }
    maxregs.at(0).primary = true;
    maxregs.at(0).enable_in_place = state.layout.in_place;
  }

  RelaxedMaxRegFuture& forKV(uint64_t kv_id, uint64_t main_server_idx,
                             std::optional<uint64_t> _my_last_ts = std::nullopt,
                             bool _measuring = false) {
    measuring = _measuring;
    max_seen_tsp = 0;

    for (uint64_t i = 0; i < state.layout.num_servers; i++) {
      auto idx = (main_server_idx + i) % state.layout.num_servers;
      auto& maxreg = maxregs.at(idx);
      maxreg.primary = i == 0;
      maxreg.enable_in_place =
          state.layout.in_place &&
          (maxreg.primary ||
           (main_server_idx < state.dead_servers && (kv_id & 1) == (i - 1)));
      maxreg.lazyness_level =
          i < state.layout.majority ? 0 : 1 + i - state.layout.majority;
      maxreg.enable_out_of_place = true;
      maxreg.forKV(kv_id, _my_last_ts, measuring);
    }

    read_entry = maxregs.at(0).getEntry();
    return *this;
  }

  void doRead() {
    step = TryRead;
    for (auto& maxreg : maxregs) {
      maxreg.doTryRead();
    }
    resetProgressMarkers();
  }

  void doReadTs() {
    step = Read;
    for (auto& maxreg : maxregs) {
      maxreg.doReadTs();
    }
    resetProgressMarkers();
  }

  void doNotifyAndReadTs(LogEntry* entry) {
    step = Read;
    for (auto& maxreg : maxregs) {
      maxreg.doNotifyAndReadTs(entry);
    }
    resetProgressMarkers();
  }

  // TODO(zyf): Allow lazily reading out-of-place on only one server ?
  void doReadLog(uint64_t client_idx, uint64_t log_id) {
    step = ReadLog;
    for (auto& maxreg : maxregs) {
      maxreg.doReadLog(client_idx, log_id);
    }
    resetProgressMarkers();
  }

  void doReplicateAndReadNotifs(LogEntry* entry) {
    step = Write;
    for (auto& maxreg : maxregs) {
      maxreg.doReplicateAndReadNotifs(entry);
    }
    resetProgressMarkers();
  }
  // TODO(zyf): Allow notifying writer of successfull replication ?

  void doReplicate(LogEntry* entry) {
    step = Write;
    for (auto& maxreg : maxregs) {
      maxreg.doReplicate(entry);
    }
    resetProgressMarkers();
  }

  void doWrite(uint64_t log_id) {
    step = Write;
    for (auto& maxreg : maxregs) {
      maxreg.doWrite(log_id);
    }
    resetProgressMarkers();
  }

  // TODO(zyf): Allow lazily writting only tsp + hash when retrying new tsp ?
  void doTryWrite(uint64_t log_id) {
    step = TryWrite;
    for (auto& maxreg : maxregs) {
      maxreg.doTryWrite(log_id);
    }
    resetProgressMarkers();
  }

  void doWriteTsAndIn() {
    step = Write;
    for (auto& maxreg : maxregs) {
      maxreg.doWriteTsAndIn();
    }
    resetProgressMarkers();
  }

  bool isLinearized() const {
    return step == Step::Done || step == Step::Linearized;
  }

  bool isDone() const { return step == Step::Done; }

  LogEntry* getMaxEntry() const { return read_entry; }

  uint64_t getMaxTsp() const { return max_seen_tsp; }

  std::optional<uint64_t> getMyTsp() const {
    std::optional <uint64_t> x = std::nullopt;
    for (auto& maxreg : maxregs) {
      if (maxreg.isLazyOrDead()) {
        continue;
      }
      auto val = maxreg.getMyTsp();
      if (x < val) {
        x = val;
      }
    }
    return x;
  }

  uint64_t getMinTsp() const {
    auto x = max_seen_tsp;
    for (auto& maxreg : maxregs) {
      if (maxreg.isLazyOrDead()) {
        continue;
      }
      auto val = maxreg.getTsp();
      if (val < x) {
        x = val;
      }
    }
    return x;
  }

  uint64_t getMyMinTsp() const {
    auto x = uint64_t(-1);
    for (auto& maxreg : maxregs) {
      if (maxreg.isLazyOrDead()) {
        continue;
      }
      auto val = maxreg.getMyTsp().value_or(0);
      if (val < x) {
        x = val;
      }
    }
    return x;
  }

  bool notified() const {
    for (auto& maxreg : maxregs) {
      if (maxreg.isLazyOrDead()) {
        continue;
      }
      if (maxreg.notified()) {
        return true;
      }
    }
    return false;
  }

  LogEntry* getWriteEntry() const { return maxregs.at(0).getWriteEntry(); }

  void addToOngoingRDMA(uint64_t server_idx, uint64_t n) {
    maxregs.at(server_idx).addToOngoingRDMA(n);
  }

  bool tryStepForward() {
    if (step == Done) {
      resetProgressMarkers();
      return true;
    }

    size_t count_ready = 0;
    size_t count_done = 0;
    size_t count_progress = 0;
    for (auto& maxreg : maxregs) {
      if (maxreg.isLazyOrDead()) {
        continue;
      }
      maxreg.tryStepForward();
      bool ready = maxreg.isDone() || (step == Write && maxreg.isLinearized());
      if (max_seen_tsp < maxreg.getTsp() ||
          (max_seen_tsp == maxreg.getTsp() && maxreg.enable_in_place)) {
        max_seen_tsp = maxreg.getTsp();
        read_entry = maxreg.getEntry();
        read_entry->kv.in_place_tsp = max_seen_tsp;
      }

      count_ready += ready ? 1 : 0;
      count_done += maxreg.isDone() ? 1 : 0;
      count_progress += ready || maxreg.made_progress ? 1 : 0;
    }

    if (count_progress < state.layout.majority) {
      return false;
    }

    // We made progress !
    resetProgressMarkers();

    if (count_ready < state.layout.majority) {
      return true;
    }

    switch (step) {
      case Write:
        if (count_done < state.layout.majority) {
          step = Linearized;
        } else {
          step = Done;
        }
        break;
      case TryRead:
        for (auto& maxreg : maxregs) {
          if (maxreg.isLazyOrDead()) {
            continue;
          }
          if (maxreg.enable_in_place && maxreg.getTsp() == max_seen_tsp &&
              (maxreg.getEntry()->kv.in_place_tsp | 1) == (max_seen_tsp | 1)) {
            chsum_t chsum;
            state.layout.hashOfTo(maxreg.getEntry()->kv, chsum);
            if (memcmp(chsum.data(), maxreg.getEntry()->kv.chsum.data(),
                       chsum.size()) == 0) {
              read_entry = maxreg.getEntry();
              step = Done;
              break;
            }
          }
        }
        // In-place read failed, retrying out-of-place
        if (step != Done) {
          step = ReadLog;
          for (auto& maxreg : maxregs) {
            maxreg.doReadLog(state.extractClientIdx(max_seen_tsp),
                             extractLogId(max_seen_tsp));
          }
          tryStepForward();
        }
        break;
      case ReadLog:
        for (auto& maxreg : maxregs) {
          if (maxreg.isLazyOrDead()) {
            continue;
          }
          if (filterLogIdAndClientProcId(maxreg.getEntry()->kv.in_place_tsp) ==
              filterLogIdAndClientProcId(max_seen_tsp)) {
            read_entry = maxreg.getEntry();
            break;
          }
        }
        // TODO(zyf): TOFIX/FIXME this is a hack. Reading logs should always succeed.
        read_entry->kv.in_place_tsp = max_seen_tsp;
        [[fallthrough]];
      case Read:
      case TryWrite:
      case Linearized:
        step = Done;
        break;
      case Done:
        break;
      default:
        throw std::runtime_error(
            "Illegal step in RelaxedMaxRegFuture. (tryStepForward)");
    }
    if (step == Done) {
      for (auto& maxreg : maxregs) {
        maxreg.markAsDone();
      }
    }
    return true;
  }
};

}  // namespace dory