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
#include "oops_state.hpp"
#include "race/client_index.hpp"
#include "tspointers.hpp"

namespace dory {

class UnreliableMaxRegFuture : public BasicFuture {
 public:
  enum Step {
    TryReadAll,
    ReadAll,
    ReadTs,
    NotifyAndReadTs,
    ReadLog,
    TryWriteAll,
    WriteAll,
    ReplicateAllAndReadNotifs,
    WriteTsAndIn,
    WriteTs,
    ReplicateTsAndReadNotifs,
    ReadNotifs,
    Dead,
    Done,
  };

 private:
  Step step = Done;
  uint64_t to_poll = 0;

  size_t server_idx;
  LogEntry* local_write_entry;
  LogEntry* local_read_entry;

  uintptr_t remote_region;

  uintptr_t remote_kv = 0;
  uint64_t ongoing_remote_kv = 0;
  uint64_t max_seen_tsp = 0;
  std::optional<uint64_t> my_last_tsp = 0;
  uintptr_t ongoing_remote_log = 0;

  LogEntry* ongoing_write_entry = nullptr;

  std::optional<Step> next_step = std::nullopt;
  LogEntry* write_entry = local_write_entry;
  uintptr_t remote_log = 0;
  void doNext(Step _step) {
    switch (_step) {
      case ReplicateAllAndReadNotifs:
      case ReplicateTsAndReadNotifs:
        if (write_entry->kv.in_place_tsp < (max_seen_tsp | 1)) {
          // KV already written. Only need to update tsp.
          return doNext(ReadNotifs);
        }
        break;
      case WriteAll:
      case TryWriteAll:
        if (write_entry->kv.in_place_tsp < (max_seen_tsp | 1)) {
          // KV already written. Only need to update tsp.
          return doNext(WriteTs);
        }
        break;
      case WriteTs:
        if (write_entry->kv.in_place_tsp < max_seen_tsp) {
          // Ts already up-to-date or newer. We're done.
          return doNext(Done);
        }
        break;
      default:
        break;
    }
    next_step = _step;
  }

 public:
  bool enable_out_of_place = true;
  bool enable_in_place = false;
  bool primary = false;
  uint64_t lazyness_level = 0;

  bool made_progress = false;

  bool measuring = false;

  UnreliableMaxRegFuture(OopsState& _state, uint64_t _logblock_id,
                         size_t _server_idx)
      : BasicFuture{_state, _logblock_id},
        server_idx{_server_idx},
        local_write_entry{state.layout.getClientWriteLog(logblock_id)},
        local_read_entry{
            state.layout.getClientReadLog(logblock_id, server_idx)} {
    dory::conn::ReliableConnection& rc =
        *state.server_connections.at(server_idx);
    remote_region = rc.remoteBuf();
  }

  UnreliableMaxRegFuture& forKV(
      uint64_t kv_id, std::optional<uint64_t> _my_last_ts = std::nullopt,
      bool _measuring = false) {
    measuring = _measuring;
    remote_kv = state.layout.getServerKVAddress(remote_region, kv_id);
    my_last_tsp = _my_last_ts;
    max_seen_tsp = _my_last_ts.value_or(0);
    return *this;
  }

  void doTryRead() { doNext(TryReadAll); }

  void doRead() { doNext(ReadAll); }

  void doReadTs() { doNext(ReadTs); }

  void doNotifyAndReadTs(LogEntry* to_notify) {
    uint64_t client_idx = state.extractClientIdx(to_notify->kv.in_place_tsp);
    uint64_t log_id = extractLogId(to_notify->kv.in_place_tsp);
    remote_log =
        state.layout.getServerLogAddress(remote_region, client_idx, log_id);
    doNext(NotifyAndReadTs);
  }

  void doReadLog(uint64_t client_idx, uint64_t log_id) {
    remote_log =
        state.layout.getServerLogAddress(remote_region, client_idx, log_id);
    doNext(ReadLog);
  }

  void doReplicate(LogEntry* to_replicate) {
    write_entry = to_replicate;
    uint64_t client_idx = state.extractClientIdx(to_replicate->kv.in_place_tsp);
    uint64_t log_id = extractLogId(to_replicate->kv.in_place_tsp);
    remote_log =
        state.layout.getServerLogAddress(remote_region, client_idx, log_id);
    doNext(WriteAll);
  }

  void doReplicateAndReadNotifs(LogEntry* to_replicate) {
    write_entry = to_replicate;
    uint64_t client_idx = state.extractClientIdx(to_replicate->kv.in_place_tsp);
    uint64_t log_id = extractLogId(to_replicate->kv.in_place_tsp);
    remote_log =
        state.layout.getServerLogAddress(remote_region, client_idx, log_id);
    doNext(ReplicateAllAndReadNotifs);
  }

  void doWrite(uint64_t log_id) {
    write_entry = local_write_entry;
    remote_log = state.layout.getServerLogAddress(remote_region,
                                                  state.client_idx, log_id);
    doNext(WriteAll);
  }

  void doTryWrite(uint64_t log_id) {
    write_entry = local_write_entry;
    remote_log = state.layout.getServerLogAddress(remote_region,
                                                  state.client_idx, log_id);
    doNext(TryWriteAll);
  }

  void doWriteTsAndIn() {
    write_entry = local_write_entry;
    doNext(WriteTsAndIn);
  }

  void markAsDone() {
    doNext(Done);
  }

  // TODO(zyf) Add option when doing 2nd attempt tsp ?

  bool isDone() const {
    return step == Step::Done && (!next_step || next_step == Step::Done);
  }

  bool isLinearized() const {
    return (step == Step::Done && !next_step) || next_step == Step::Done || (max_seen_tsp | 1) >= write_entry->kv.in_place_tsp;
  }

  LogEntry* getEntry() const { return local_read_entry; }

  uint64_t getTsp() const { return max_seen_tsp; }

  std::optional<uint64_t> getMyTsp() const { return my_last_tsp; }

  bool notified() const { return local_read_entry->notif != 0; }

  LogEntry* getWriteEntry() const { return local_write_entry; }

  void addToOngoingRDMA(uint64_t n) {
    state.to_poll_from_connections.at(server_idx) += n;
    to_poll += n;
  }

 private:
  uint64_t* swapback() const { return &(local_read_entry->from_tsp); };
  uint64_t* notifSwapback() const { return &(local_read_entry->notif); };

  bool transitionTo(Step _step) {
    switch (_step) {
      case ReplicateAllAndReadNotifs:
      case ReplicateTsAndReadNotifs:
        if (write_entry->kv.in_place_tsp <= (max_seen_tsp | 1)) {
          // KV already written. Only need to update tsp.
          return transitionTo(ReadNotifs);
        }
        break;
      case WriteAll:
      case TryWriteAll:
        if (write_entry->kv.in_place_tsp <= (max_seen_tsp | 1)) {
          // KV already written. Only need to update tsp.
          return transitionTo(WriteTs);
        }
        break;
      case WriteTs:
        if (write_entry->kv.in_place_tsp <= max_seen_tsp) {
          // Ts already up-to-date or newer. We're done.
          return transitionTo(Done);
        }
        break;
      default:
        break;
    }

    step = _step;
    if (step == Done) {
      return true;
    }

    dory::conn::ReliableConnection& rc =
        *state.server_connections.at(server_idx);

    ongoing_remote_kv = remote_kv;
    ongoing_write_entry = write_entry;
    ongoing_remote_log = remote_log;

    if(measuring)
      state.unreliable_maxreg_steps_counts.at(static_cast<size_t>(step))++;

    switch (step) {
      case TryReadAll:
      case ReadAll:
        if (enable_in_place) {
          rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead,
                            futureId(), &(local_read_entry->kv),
                            state.layout.fullKVSize(), remote_kv);
          addToOngoingRDMA(1);
          break;
        }
        [[fallthrough]];
      case ReadTs:
      case NotifyAndReadTs:
        if (step == NotifyAndReadTs) {
          rc.postSendSingleCas(futureId(), notifSwapback(),
                               remote_log + offsetof(LogEntry, notif), 0, 1,
                               false);
        }
        rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead, futureId(),
                          state.layout.tspArrayOf(local_read_entry->kv),
                          state.layout.tspArraySize(),
                          remote_kv + state.layout.tspArrayOffset());
        addToOngoingRDMA(1);
        break;
      case ReadLog:
        rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead, futureId(),
                          &(local_read_entry->kv), state.layout.partialKVSize(),
                          remote_log + offsetof(LogEntry, kv));
        addToOngoingRDMA(1);
        break;
      case TryWriteAll:
      case WriteAll:
      case ReplicateAllAndReadNotifs: {
        struct ibv_send_wr wr[4];
        struct ibv_sge sg[4];
        uint64_t wr_id = 0;
        // Prepare work requests
        
        if (step == TryWriteAll && state.layout.num_tsp > 1) {
          // Read tsp
          rc.prepareSingle(wr[wr_id], sg[wr_id],
                           dory::conn::ReliableConnection::RdmaRead, futureId(),
                           state.layout.tspArrayOf(local_read_entry->kv),
                           state.layout.tspArraySize(),
                           remote_kv + state.layout.tspArrayOffset(), false);
          wr_id++;
        }

        if (enable_out_of_place) {
          // Write out-of-place
          auto offset = offsetof(LogEntry, from_tsp);
          auto size = state.layout.partialLogEntrySize() -
                      static_cast<uint32_t>(offset);
          rc.prepareSingle(wr[wr_id], sg[wr_id],
                           dory::conn::ReliableConnection::RdmaWrite,
                           futureId(), &(write_entry->from_tsp), size,
                           remote_log + offset, false);
          wr_id++;
        }

        // Write tsp
        rc.prepareSingleCas(wr[wr_id], sg[wr_id], futureId(), swapback(),
                            remote_kv + state.layout.clientTspOffset(state.client_idx),
                            my_last_tsp.value_or(0), write_entry->kv.in_place_tsp,
                            true);
        addToOngoingRDMA(1);
        wr_id++;

        const bool first_write = !my_last_tsp;
        const bool indexing =
            primary &&
            first_write;  // Write key in-place to allow faster index-search
        const bool replicating = write_entry != local_write_entry;
        const bool lazy_replication =
            replicating &&
            enable_out_of_place;  // Original writer will likely write in-place.
        const bool lazy_try_write = step == TryWriteAll && enable_out_of_place;
        const bool in_place =
            indexing ||
            (enable_in_place && !lazy_replication && !lazy_try_write);
        if (in_place) {
          // Write in-place
          rc.prepareSingle(wr[wr_id], sg[wr_id],
                           dory::conn::ReliableConnection::RdmaWrite,
                           futureId(), &(write_entry->kv),
                           state.layout.partialKVSize(), remote_kv, false);
          wr_id++;
        }

        // Send work requests
        if (state.layout.doorbell) {
          for (uint64_t i = 1; i < wr_id; i++) {
            wr[i - 1].next = &wr[i];
          }
          rc.postSend(wr[0]);
        } else {
          for(uint64_t i = 0; i < wr_id; i++) {
            rc.postSend(wr[i]);
          }
        }
      } break;
      case WriteTsAndIn:
      case WriteTs:
      case ReplicateTsAndReadNotifs:
        rc.postSendSingleCas(
            futureId(), swapback(),
            remote_kv + state.layout.clientTspOffset(state.client_idx),
            my_last_tsp.value_or(0), write_entry->kv.in_place_tsp);
        addToOngoingRDMA(1);
        if (step == WriteTsAndIn && enable_in_place) {
          rc.postSendSingle(
              dory::conn::ReliableConnection::RdmaWrite, futureId(),
              &(write_entry->kv), state.layout.partialKVSize(),
              remote_kv, false);
        }
        break;
      case ReadNotifs:
      case Dead:
      case Done:
        break;
      default:
        throw std::runtime_error(
            "Illegal step in UnreliableMaxRegFuture. (transitionTo)");
    }

    switch (step) {
      case ReplicateAllAndReadNotifs:
      case ReplicateTsAndReadNotifs:
      case ReadNotifs:
        rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead, futureId(),
                          &(local_read_entry->notif), sizeof(uint64_t),
                          remote_log + offsetof(LogEntry, notif));
        addToOngoingRDMA(1);
        break;
      default:
        break;
    }

    return true;
  }

 public:
  bool isLazy() const {
    return state.dead_servers < lazyness_level;
  }
  bool isDead() const {
    return server_idx < state.dead_servers;
  }
  bool isLazyOrDead() const {
    return isLazy() || isDead();
  }

  bool tryStepForward() {
    if (isDead()) {
      step = Dead;
      return false;  // Server is dead
    }
    if (isLazy()) {
      return false; // Enough servers are alive to be lazy
    }

    if (to_poll > (ongoing_remote_kv != remote_kv ? 1 : 0)) {
      return false;
    }

    if (ongoing_remote_kv == remote_kv) {
      // Update last seen tsp
      switch (step) {
        case TryWriteAll:
        case WriteAll:
        case ReplicateAllAndReadNotifs:
        case WriteTsAndIn:
        case WriteTs:
        case ReplicateTsAndReadNotifs:
          if (my_last_tsp.value_or(0) == *swapback()) {
            my_last_tsp = ongoing_write_entry->kv.in_place_tsp;
          } else {
            my_last_tsp = *swapback();
          }
          state.layout.myTspOf(ongoing_write_entry->kv, state.client_idx) =
              my_last_tsp.value();
          if (max_seen_tsp < *my_last_tsp) {
            max_seen_tsp = *my_last_tsp;
            state.syncClock(max_seen_tsp);
          }
          if (step != TryWriteAll || state.layout.num_tsp == 1) {
            break;
          }
          [[fallthrough]];
        case TryReadAll:
        case ReadAll:
        case ReadTs:
        case NotifyAndReadTs: {
          auto new_max = state.layout.maxTspOf(local_read_entry->kv);
          if (max_seen_tsp < new_max) {
            max_seen_tsp = new_max;
            state.syncClock(new_max);
          }
          if (step != TryWriteAll) {
            my_last_tsp =
                state.layout.myTspOf(local_read_entry->kv, state.client_idx);
          }
          break;
        }
        case ReadLog:
        case ReadNotifs:
        case Dead:
        case Done:
          break;
        default:
          throw std::runtime_error(
              "Illegal step in UnreliableMaxRegFuture. (tryStepForward)");
      }
    }

    if (next_step) {
      auto newStep = *next_step;
      next_step = std::nullopt;
      transitionTo(newStep);
      return true;
    }
    made_progress = true;

    if (step == Done) {
      return true;
    }

    switch (step) {
      case TryReadAll:
        transitionTo(Done);
        break;
      case ReadAll:
        if (max_seen_tsp == 0) {
          transitionTo(Done);
          break;
        }
        if (enable_in_place && (local_read_entry->kv.in_place_tsp | 1) == (max_seen_tsp | 1)) {
          chsum_t chsum;
          state.layout.hashOfTo(local_read_entry->kv, chsum);
          if (memcmp(chsum.data(), local_read_entry->kv.chsum.data(),
                     chsum.size()) == 0) {
            transitionTo(Done);
            break;
          }
        }
        remote_log = state.layout.getServerLogAddress(
            remote_region, state.extractClientIdx(max_seen_tsp),
            extractLogId(max_seen_tsp));
        transitionTo(ReadLog);
        break;
      case ReadTs:
      case NotifyAndReadTs:
      case ReadLog:
        transitionTo(Done);
        break;
      case TryWriteAll:
        transitionTo(Done);
        break;
      case WriteAll:
      case WriteTsAndIn:
      case WriteTs:
        transitionTo(WriteTs);
        break;
      case ReplicateAllAndReadNotifs:
      case ReplicateTsAndReadNotifs:
        if (write_entry->kv.in_place_tsp < (max_seen_tsp | 1)) {
          transitionTo(Done);
        } else {
          transitionTo(ReplicateTsAndReadNotifs);
        }
        break;
      case ReadNotifs:
        transitionTo(Done);
        break;
      case Dead:
      case Done:
        break;
      default:
        throw std::runtime_error(
            "Illegal step in UnreliableMaxRegFuture. (tryStepForward)");
    }

    return true;
  }
};

}  // namespace dory