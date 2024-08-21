#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/shared/logger.hpp>

#include "latency.hpp"
#include "layout.hpp"
#include "main.hpp"  // TODO(zyf): remove/rename/whatever
#include "oops_future.hpp"
#include "race/client_index.hpp"
#include "tspointers.hpp"

namespace dory {

class OopsClient {
 private:
  OopsState state;
  std::vector<OopsFuture> futures;
  std::vector<bool> progress;

  // TODO(zyf): TOFIX: LOGGER_DECL_INIT(logger, "OopsClient");

 public:
  OopsClient(Layout _layout, conn::RcConnectionExchanger<ProcId>& rcx,
             ProcId _proc_id, uint64_t pointer_cache_size,
             bool measure_batches = false, uint64_t death_point = uint64_t(-1),
             uint64_t iter_count = 10000000)
      : state{_layout,         rcx,         _proc_id,  pointer_cache_size,
              measure_batches, death_point, iter_count} {
    state.unreliable_maxreg_steps_counts.resize(
        static_cast<size_t>(UnreliableMaxRegFuture::Step::Done) + 1);
    state.oops_data_steps_counts.resize(
        static_cast<size_t>(OopsDataFuture::Step::Done) + 1);
    // Prepare futures
    progress.resize(state.layout.async_parallelism);
    futures.reserve(state.layout.async_parallelism);
    for (size_t i = 0; i < state.layout.async_parallelism; ++i) {
      futures.emplace_back(state, i);
      progress.at(i) = false;
    }
  }

  bool tickRdma() {
    bool any_progress = false;
    for (size_t i = 0; i < state.layout.num_servers; ++i) {
      auto& rc = *state.server_connections.at(i);
      auto& to_poll = state.to_poll_from_connections.at(i);
      state.wces.resize(to_poll);
      if (!rc.pollCqIsOk(conn::ReliableConnection::SendCq, state.wces)) {
        throw std::runtime_error("Error polling cq");
      }
      for (auto const& wc : state.wces) {
        if (wc.status != IBV_WC_SUCCESS) {
          // TODO(zyf): TOFIX: logger->error("RDMA WC status failure: {}",
          // wc.status);
          throw std::runtime_error("WC unsuccessful.");
        }
        // TODO(zyf): TOFIX: logger->info("RDMA {} completed", wc.wr_id);
        futures.at(wc.wr_id).addToOngoingRDMA(i, -1);
        progress.at(wc.wr_id) = true;
        any_progress = true;
      }
    }
    return state.index->tickRdma(progress) || any_progress;
  }

  OopsFuture& getFuture(uint64_t i) {
    return futures.at(i % state.layout.async_parallelism);
  }

  OopsFuture& getFreeFuture() {
    while (true) {
      for (uint64_t i = 0; i < state.layout.async_parallelism; i++) {
        auto& future = futures.at(i);
        if (progress.at(i)) {
          future.tryStepForward();
          progress.at(i) = false;
        }
        if (future.isDone()) {
          return future;
        }
      }
      tickRdma();
    }
  }

  void finishAllFutures() {
    for (auto& target : futures) {
      while (!target.isDone()) {
        tickRdma();
        for (uint64_t i = 0; i < state.layout.async_parallelism; i++) {
          auto& future = futures.at(i);
          if (progress.at(i)) {
            future.tryStepForward();
            progress.at(i) = false;
          }
        }
      }
    }
  }

  void initClock() {
    state.syncClock(0, true);
  }

  void startMeasurements(timepoint start) {
    state.startMeasurements(start);
  }

  void reportStats(bool detailed = false) {
    fmt::print("\n");
    fmt::print("################ Step-counts stats:\n");
    fmt::print("######## UnreliableMaxRegFuture steps counts:\n");
    for (size_t i = 0; i < state.unreliable_maxreg_steps_counts.size(); i++) {
      if (state.unreliable_maxreg_steps_counts[i] == 0) continue;
      std::string_view name = "unknown";
      switch (static_cast<UnreliableMaxRegFuture::Step>(i)) {
        case dory::UnreliableMaxRegFuture::TryReadAll:
          name = "TryReadAll";
          break;
        case dory::UnreliableMaxRegFuture::ReadAll:
          name = "ReadAll";
          break;
        case dory::UnreliableMaxRegFuture::ReadTs:
          name = "ReadTs";
          break;
        case dory::UnreliableMaxRegFuture::NotifyAndReadTs:
          name = "NotifyAndReadTs";
          break;
        case dory::UnreliableMaxRegFuture::ReadLog:
          name = "ReadLog";
          break;
        case dory::UnreliableMaxRegFuture::TryWriteAll:
          name = "TryWriteAll";
          break;
        case dory::UnreliableMaxRegFuture::WriteAll:
          name = "WriteAll";
          break;
        case dory::UnreliableMaxRegFuture::ReplicateAllAndReadNotifs:
          name = "ReplicateAllAndReadNotifs";
          break;
        case dory::UnreliableMaxRegFuture::WriteTsAndIn:
          name = "WriteTsAndIn";
          break;
        case dory::UnreliableMaxRegFuture::WriteTs:
          name = "WriteTs";
          break;
        case dory::UnreliableMaxRegFuture::ReplicateTsAndReadNotifs:
          name = "ReplicateTsAndReadNotifs";
          break;
        case dory::UnreliableMaxRegFuture::ReadNotifs:
          name = "ReadNotifs";
          break;
        case dory::UnreliableMaxRegFuture::Done:
          name = "Done";
          break;
        default:
          break;
      }
      fmt::print("{}: {}\n", name,
                 state.unreliable_maxreg_steps_counts[i]);
    }
    fmt::print("######## OopsDataFuture steps counts:\n");
    for (size_t i = 0; i < state.oops_data_steps_counts.size(); i++) {
      if (state.oops_data_steps_counts[i] == 0) continue;
      std::string_view name = "unknown";
      switch (static_cast<OopsDataFuture::Step>(i)) {
        case dory::OopsDataFuture::WTryOptimisticTs:
          name = "WTryOptimisticTs";
          break;
        case dory::OopsDataFuture::WPrepareToRepare:
          name = "WPrepareToRepare";
          break;
        case dory::OopsDataFuture::WTryRepare:
          name = "WTryRepare";
          break;
        case dory::OopsDataFuture::WReadToWrite:
          name = "WReadToWrite";
          break;
        case dory::OopsDataFuture::RReading:
          name = "RReading";
          break;
        case dory::OopsDataFuture::RTryNotifyWriter:
          name = "RTryNotifyWriter";
          break;
        case dory::OopsDataFuture::ReplicateAsVerified:
          name = "ReplicateAsVerified";
          break;
        case dory::OopsDataFuture::WriteTsAsVerified:
          name = "WriteTsAsVerified";
          break;
        case dory::OopsDataFuture::WriteAsVerified:
          name = "WriteAsVerified";
          break;
        case dory::OopsDataFuture::Linearized:
          name = "Linearized";
          break;
        case dory::OopsDataFuture::Done:
          name = "Done";
          break;
        default:
          break;
      }
      fmt::print("{}: {}\n", name, state.oops_data_steps_counts[i]);
    }

    state.reportStats(detailed);
  }
};  // class OopsClient

}  // namespace dory