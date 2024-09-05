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
#include "relaxed_maxreg.hpp"
#include "tspointers.hpp"

namespace dory {

class OopsDataFuture : public BasicFuture {
 public:
  enum Step {
    // Write steps:
    WTryOptimisticTs,  // When trying to write optistimistically.
    WPrepareToRepare,  // Probably failed optimistic write, read other's entry.
    WTryRepare,        // Try to fix registers before retrying.
    WReadToWrite,      // Don't write optimistically, read last ts.
    // Read steps:
    RReading,          // Read
    RTryNotifyWriter,  // Try notifying writer (to not retry) before
                       // confirmation
    // Common steps:
    ReplicateAsVerified,
    WriteTsAsVerified,
    WriteAsVerified,  // Writting or replicating with ts marked as verified
    Linearized,  // Already linearized, continue confirmation in background.
    Done         // Finished operation
    // TODO(zyf): Add step to repare in-place kv-pair in the background ?
  };
 private:
  Step step = Done;
  RelaxedMaxRegFuture maxreg;

  std::vector<std::optional<uint64_t>> seen;

  uint64_t log_id = 0;
  uint64_t m_ts = 0;
  uint64_t previous_ts = 0;

  bool measuring = false;

 public:
  std::optional<timepoint> linearization_time = std::nullopt;

  // Note: when writing, entry->from_tsp is assumed to be containing the expected
  // timestamp !
  OopsDataFuture(OopsState& _state, size_t _logblock_id)
      : BasicFuture{_state, _logblock_id}, maxreg{state, _logblock_id} {
    seen.resize(state.layout.num_clients, std::nullopt);
  }

  OopsDataFuture& forKV(uint64_t kv_id, uint64_t main_server_idx,
                        std::optional<uint64_t> _my_last_ts = std::nullopt,
                        bool _measuring = false) {
    measuring = _measuring;
    linearization_time = std::nullopt;
    m_ts = _my_last_ts.value_or(0);
    previous_ts = 0;
    maxreg.forKV(kv_id, main_server_idx, _my_last_ts, measuring);
    return *this;
  }

  void doWrite(uint64_t _log_id) {
    log_id = _log_id;
    transitionTo(WTryOptimisticTs);
  }

  void doRead() {
    for (auto& s : seen) {
      s = std::nullopt;
    }
    transitionTo(RReading);
  }

  bool isLinearized() const {
    return step == Step::Done || step == Step::Linearized;
  }

  bool isDone() const { return step == Step::Done; }

  LogEntry* getEntry() const { return maxreg.getMaxEntry(); }

  uint64_t getTsp() const { return maxreg.getMaxTsp(); }

  std::optional<uint64_t> getMyTsp() const { return maxreg.getMyTsp(); }

  LogEntry* getWriteEntry() const { return maxreg.getWriteEntry(); }

  void addToOngoingRDMA(uint64_t server_idx, uint64_t n) {
    maxreg.addToOngoingRDMA(server_idx, n);
  }

  bool transitionTo(Step _step) {
    if (_step == WTryOptimisticTs &&
        (!state.layout.guess_ts ||
         ((state.layout.num_tsp << 1) <= state.layout.num_clients &&
             getMyTsp().value_or(1) == 0))) {
      return transitionTo(WReadToWrite);
    }

    step = _step;
    if(!isLinearized() && measuring) {
      state.oops_data_steps_counts[static_cast<size_t>(step)]++;
    }

    switch (step) {
      // Write steps:
      case WTryOptimisticTs: {
        // Guess ts
        maxreg.getWriteEntry()->kv.in_place_tsp =
            makeTsp(state.newTs(), log_id, state.proc_id, false);
        // Try write
        maxreg.doTryWrite(log_id);
        break;
      }
      case WPrepareToRepare: {
        // TODO(zyf): TOFIX Handle server failure (read all logs ?)
        // Read log of conflicting ts
        auto tsp = maxreg.getMaxTsp();
        maxreg.doReadLog(state.extractClientIdx(tsp), extractLogId(tsp));
        break;
      }
      case WTryRepare:
        // M.write(m) & self.readNotifs(x)
        maxreg.doReplicateAndReadNotifs(maxreg.getMaxEntry());
        break;
      case WReadToWrite:
        maxreg.doReadTs();
        break;
      // Read steps:
      case RReading:
        previous_ts = m_ts;
        maxreg.doRead();  // TODO(zyf): TOFIX, Always read all ?
        break;
      case RTryNotifyWriter:
        maxreg.doNotifyAndReadTs(maxreg.getMaxEntry());
        break;
      // Common steps:
      case ReplicateAsVerified:
        getEntry()->kv.in_place_tsp |= 1;
        maxreg.doReplicate(getEntry());
        break;
      case WriteTsAsVerified:
        getWriteEntry()->kv.in_place_tsp |= 1;
        maxreg.doWriteTsAndIn();
        break;
      case WriteAsVerified:
        // TODO(zyf): TOFIX, optimize and only rewrite log partially when
        // possible...
        getWriteEntry()->kv.in_place_tsp |= 1;
        maxreg.doWrite(log_id);  // TODO(zyf): TOFIX, pass entry as argument ?
                                 // (when replicating ??)
        break;
      case Linearized:  // Already linearized, continue in background.
      case Done:
        if (measuring && !linearization_time) {
          linearization_time = std::chrono::steady_clock::now();
        }
        break;
      // TODO(zyf): Add step to repare in-place kv-pair in the background ?
      default:
        throw std::runtime_error(
            "Illegal step in OopsDataFuture. (transitionTo)");
    }

    tryStepForward();
    return true;
  }

  bool tryStepForward() {
    if (!maxreg.tryStepForward()) {
      return false;
    }

    if (!maxreg.isLinearized() || (step == Linearized && !maxreg.isDone())) {
      return true;
    }

    switch (step) {
      // Write steps:
      case WTryOptimisticTs:
        m_ts = maxreg.getMaxTsp();
        if (m_ts <= (getWriteEntry()->kv.in_place_tsp | 1)) {
          if (measuring &&
              getWriteEntry()->kv.in_place_tsp <= maxreg.getMinTsp()) {
            linearization_time = std::chrono::steady_clock::now();
          }
          transitionTo(WriteTsAsVerified);
        } else {
          if (maxreg.getMinTsp() <= getWriteEntry()->kv.in_place_tsp) {
            transitionTo(WPrepareToRepare);
          } else if (maxreg.getMyMinTsp() <= getWriteEntry()->kv.in_place_tsp) {
            transitionTo(WTryRepare);
          } else {
            getWriteEntry()->kv.in_place_tsp =
                makeTsp(state.newTs(), log_id, state.proc_id, true);
            transitionTo(WriteTsAsVerified);
          }
        }
        break;
      case WPrepareToRepare:
        // TODO(zyf): TOFIX, Handle if reading log from server failed ?
        transitionTo(WTryRepare);
        break;
      case WTryRepare:
        if (maxreg.notified()) {
          if (maxreg.isDone()) {
            transitionTo(Done);
          } else {
            transitionTo(Linearized);
          }
        } else {
          getWriteEntry()->kv.in_place_tsp =
              makeTsp(state.newTs(), log_id, state.proc_id, true);
          transitionTo(WriteTsAsVerified);
        }
        break;
      case WReadToWrite:
        m_ts = maxreg.getMaxTsp();
        getWriteEntry()->kv.in_place_tsp =
            makeTsp(state.newTs(), log_id, state.proc_id, true);
        transitionTo(WriteAsVerified);
        break;
      // Read steps:
      case RTryNotifyWriter:
        m_ts = maxreg.getMaxTsp();
        if (m_ts == previous_ts) {
          transitionTo(ReplicateAsVerified);
        }
        [[fallthrough]];
      case RReading:
        m_ts = maxreg.getMaxTsp();
        if (isVerified(m_ts)) {
          transitionTo(ReplicateAsVerified);
        } else if (m_ts == previous_ts) {
          transitionTo(RTryNotifyWriter);
        } else {
          auto c = state.extractClientIdx(m_ts);
          if (seen.at(c)) {
            m_ts = *seen.at(c);
            // TODO(zyf): TOFIX: Actually recover this client's entry if not
            // stored locally ?
            transitionTo(Linearized);
          } else {
            seen.at(c) = m_ts;
            transitionTo(RReading);
          }
        }
        break;
      // Common steps:
      case ReplicateAsVerified:
      case WriteTsAsVerified:
      case WriteAsVerified:  // Writting or replicating with tsp marked as
                             // confirmed
        if (maxreg.isDone()) {
          transitionTo(Done);
        } else {
          transitionTo(Linearized);
        }
        break;
      case Linearized:  // Already linearized, continue confirmation in
                        // background.
        if (maxreg.isDone()) {
          transitionTo(Done);
        }
        break;
      case Done:  // Finished operation
        break;
      // TODO(zyf): Add step to repare in-place kv-pair in the background ?
      default:
        throw std::runtime_error(
            "Illegal step in OopsDataFuture. (tryStepForward)");
    }
    return true;
  }  // void tryStepForward()
};   // class OopsDataFuture

}  // namespace dory