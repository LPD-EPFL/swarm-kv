#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>

#include <memory>
#include <set>
#include <stdexcept>
#include <variant>
#include <vector>

#include <fmt/core.h>
#include <hipony/enumerate.hpp>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/extern/ibverbs.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/move-indicator.hpp>
#include <dory/shared/types.hpp>

#include "../base.hpp"
#include "buckets.hpp"

namespace dory::race {

class IndexClient : public std::enable_shared_from_this<IndexClient> {
 public:
  using Value = dory::race::Value;

 private:
  LOGGER_DECL_INIT(logger, "IndexClient");
  size_t bucket_bits;

  struct WrContext {
    uint64_t id;
    uint64_t ongoing_rdma = 0;
    uint8_t* rdma_buffer;

    WrContext(uint64_t id, uint8_t* const rdma_buffer)
        : id{id}, rdma_buffer{rdma_buffer} {}

    // Search
    using IndexBuffer = std::array<BucketGroup, 2>;
    IndexBuffer& getBuffer() const {
      return *reinterpret_cast<IndexBuffer*>(rdma_buffer);
    }

    BucketEntry& getSwapback() const {
      return *reinterpret_cast<BucketEntry*>(rdma_buffer);
    }

    static size_t constexpr BufferSize = sizeof(IndexBuffer);
  };

  static size_t constexpr NbContexts = conn::ReliableConnection::WrDepth;
  std::vector<WrContext> wr_ctxs;

  std::vector<struct ibv_wc> wce;
  size_t to_poll = 0;

  WrContext& getWrCtx(uint64_t wr_id) { return wr_ctxs.at(wr_id); }

  static BucketEntry::Checksum checksum(HashedKey const& hkey) {
    return hkey[0] ^ hkey[7] ^ hkey[8] ^ hkey[15];
  }

  std::array<uint64_t, 2> bucketOffsets(HashedKey const& hkey) const {
    size_t const mask = (1 << bucket_bits) - 1;
    std::array<uint64_t, 2> bucket_ids;
    bucket_ids[0] = reinterpret_cast<uint64_t const*>(hkey.data())[0] & mask;
    bucket_ids[1] =
        reinterpret_cast<uint64_t const*>(hkey.data())[1] % (mask - 1);
    if ((bucket_ids[1] & ~1) >= (bucket_ids[0] & ~1)) {
      bucket_ids[1] += 2;
    }
    assert(bucket_ids[0] != bucket_ids[1]);
    assert((bucket_ids[1] & mask) == bucket_ids[1]);
    return {bucket_ids[0], bucket_ids[1]};
  }

  // RDMA
  ctrl::OpenDevice device;
  Delayed<ctrl::ControlBlock> cb;
  Delayed<dory::conn::RcConnectionExchanger<ProcId>> ce;
  std::vector<dory::conn::ReliableConnection*> server_connections;

 public:
  IndexClient(ProcId const id, uint64_t num_servers, size_t const bucket_bits, std::string const& name = "default")
      : bucket_bits{bucket_bits},
        device{std::move(ctrl::Devices().list().back())} {
    static size_t constexpr AllocatedSize = WrContext::BufferSize * NbContexts;

    ctrl::ResolvedPort port{device};
    if (!port.bindTo(0)) {
      throw std::runtime_error("Couldn't bind the device.");
    }
    cb.emplace(port);
    cb->registerPd("pd");
    cb->allocateBuffer("buf", AllocatedSize, 64);
    cb->registerMr(
        "mr", "pd", "buf",
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
            ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
            ctrl::ControlBlock::REMOTE_ATOMIC);
    cb->registerCq("index-cq");

    // 4. We establish reliable connections.
    auto& store = memstore::MemoryStore::getInstance();

    std::vector<ProcId> server_ids;
    server_ids.reserve(num_servers);
    for (uint64_t i = 1; i <= num_servers; i++) {
      server_ids.push_back(i);
    }
    ce.emplace(id, server_ids, *cb);
    auto const topic = fmt::format("{}-race-qp", name);
    ce->configureAll("pd", "mr", "index-cq", "index-cq");
    ce->announceAll(store, topic);
    ce->unannounceReady(store, topic,
                        "connected");  // Clean up from previous runs
    ce->announceReady(store, topic, "prepared");
    logger->info("Connecting...");
    ce->waitReadyAll(store, topic, "prepared");
    ce->connectAll(
        store, topic,
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
            ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
            ctrl::ControlBlock::REMOTE_ATOMIC);
    logger->info("Connected!");
    logger->info("RDMA buffer: {} to {}", cb->mr("mr").addr,
                 cb->mr("mr").addr + cb->mr("mr").size);

    auto buffer = cb->mr("mr").addr;
    wr_ctxs.reserve(NbContexts);
    for (size_t i = 0; i < NbContexts; i++) {
      wr_ctxs.emplace_back(i, reinterpret_cast<uint8_t*>(buffer));
      buffer += WrContext::BufferSize;
    }
    ce->announceReady(store, topic, "connected");
    ce->waitReadyAll(store, topic, "connected");
    // Clean up for the next run.
    ce->unannounceAll(store, topic);
    ce->unannounceReady(store, topic, "prepared");

    server_connections.reserve(server_ids.size());
    for (auto const& server_id : server_ids) {
      server_connections.emplace_back(&ce->connections().at(server_id));
    }
  }

  class BasicFuture {
   public:
    BasicFuture(std::shared_ptr<IndexClient> _client, uint64_t id)
        : client{std::move(_client)}, ctx{this->client->getWrCtx(id)} {}
    BasicFuture(BasicFuture const&) = delete;
    BasicFuture& operator=(BasicFuture const&) = delete;
    BasicFuture(BasicFuture&&) noexcept = default;
    BasicFuture& operator=(BasicFuture&&) noexcept = default;

   protected:
    std::shared_ptr<IndexClient> client;
    WrContext& ctx;
    MoveIndicator moved;

    uint64_t ctxWrId() { return reinterpret_cast<uint64_t>(ctx.id); }
  };

  bool tickRdma(std::vector<bool>& progress) {
    bool any_progress = false;
    wce.resize(to_poll);
    auto* rc = server_connections.at(0);
    if (!rc->pollCqIsOk(conn::ReliableConnection::SendCq, wce)) {
      throw std::runtime_error("Error polling cq");
    }
    for (auto const& wc : wce) {
      if (wc.status != IBV_WC_SUCCESS) {
        logger->error("RDMA WC status failure: {}", wc.status);
        throw std::runtime_error("WC unsuccessful.");
      }
      // logger->info("RDMA {} completed", wc.wr_id);
      wr_ctxs.at(wc.wr_id).ongoing_rdma -= 1;
      progress.at(wc.wr_id) = true;
      any_progress = true;
    }
    to_poll -= wce.size();
    return any_progress;
  }

  // Search
  struct IndexResult {
    size_t nb_matches = 0;
    std::array<std::optional<Value>, NbBucketEntries * 2 * 2> matches;
    size_t nb_free = 0;
    uintptr_t first_free;
    bool updated = false;
  };

  class IndexFuture : public BasicFuture {
   private:
    enum Step { Searching, Inserting, Done };
    Step step = Step::Done;
    uintptr_t remote_read_address[2];
    bool bucket_side[2];

    struct ibv_send_wr wr[2];
    struct ibv_sge sg[2];

    BucketEntry::Checksum check;
    IndexResult result;
    BucketEntry to_insert{0, 0};
    uint64_t main_server_idx;

   public:
    IndexFuture(const std::shared_ptr<IndexClient> _client, uint64_t id)
        : BasicFuture{std::move(_client), id} {}

    void retrySearch() {
      step = Step::Searching;
      result = {};

      auto& bucketgroups = ctx.getBuffer();

      auto* rc = client->server_connections.at(main_server_idx);
      ctx.ongoing_rdma += 1;
      client->to_poll += 1;
      for (auto const group : {0u, 1u}) {
        rc->prepareSingle(wr[group], sg[group],
                          conn::ReliableConnection::RdmaRead, ctxWrId(),
                          &(bucketgroups[group]), sizeof(BucketGroup),
                          remote_read_address[group], group == 1);
        rc->postSend(wr[group]);
      }
      // TODO(zyf): TOFIX, doorbell batch ?
      // wr[0].next = &wr[1];
      // client->rc->postSend(wr[0]);
    }

    void search(HashedKey const& hkey) {
      main_server_idx = *reinterpret_cast<uint64_t const*>(hkey.data()) %
                        client->server_connections.size();
      check = checksum(hkey);

      auto& bucketgroups = ctx.getBuffer();
      auto* rc = client->server_connections.at(main_server_idx);

      auto const bucket_ids = client->bucketOffsets(hkey);
      for (auto const group : {0u, 1u}) {
        bucket_side[group] = (bucket_ids[group] & 1) == 1;
        auto const bucket_location =
            (bucket_ids[group] >> 1) * 3 + (bucket_side[group] ? 1 : 0);
        remote_read_address[group] =
            rc->remoteBuf() + bucket_location * sizeof(Bucket);
      }

      retrySearch();
    }

    void tryInsert(Value const value) {
      to_insert = {check, value};
      if (result.nb_free == 0) {
        throw std::runtime_error("Bucket ran out of space!!!");
      }
      // client->logger->info("RDMA-casing 0 -> {} at {}, wr_id={}",
      // to_insert.as_uint64(), result.first_free, ctx_wr_id());
      ctx.ongoing_rdma += 1;
      auto* rc = client->server_connections.at(main_server_idx);
      rc->postSendSingleCas(ctxWrId(), &(ctx.getSwapback()), result.first_free,
                            0, to_insert.asUint64());
      client->to_poll++;
    }

    bool isDone() const { return step == Step::Done; }

    bool tryStepForward() {
      if (ctx.ongoing_rdma) {
        return false;
      }
      switch (step) {
        case Step::Searching: {
          auto& bucketgroups = ctx.getBuffer();
          uintptr_t first_free[2] = {0UL, 0UL};
          uint32_t group_free_nb[2] = {0U, 0U};

          // Traverse non-overflow buckets first
          for (auto const is_overflow : {false, true}) {
            for (auto const& [g, bucketgroup] :
                 hipony::enumerate(bucketgroups)) {
              bool b = (bucket_side[g] != is_overflow);  // XOR
              auto bucket = bucketgroup[b];

              for (auto const& [e, entry] : hipony::enumerate(bucket)) {
                if (!entry.used) {
                  if (group_free_nb[g]++ == 0) {
                    auto const& bg =
                        *reinterpret_cast<BucketGroup*>(remote_read_address[g]);
                    first_free[g] = reinterpret_cast<uintptr_t>(&(bg[b][e]));
                  }
                } else if (entry.checksum == check) {
                  result.matches[result.nb_matches++] = entry.getValue();
                }
              }
            }
          }

          result.nb_free = group_free_nb[0] + group_free_nb[1];
          // client->logger->info("Free slots: {}+{}", group_free_nb[0],
          // group_free_nb[1]); Return the first free slot in the least full
          // bucketgroup
          result.first_free = first_free[group_free_nb[0] < group_free_nb[1]];

          step = Step::Done;

          // TODO(zyf): TOFIX? also cache free slots as bytes (insert
          // optimization)
          break;
        }
        case Step::Inserting: {
          if (ctx.getSwapback().asUint64() == 0) {  // The CAS succeeded
            result.matches[result.nb_matches++] = to_insert.getValue();
            result.updated = true;
          }

          step = Step::Done;
          break;
        }
        default: {
        }
      }
      return true;
    }

    IndexResult const& get() const { return result; }

    std::optional<uintptr_t> getAddressOf(BucketEntry const& target_entry) {
      auto& bucketgroups = ctx.getBuffer();
      for (auto const& [g, bucketgroup] : hipony::enumerate(bucketgroups)) {
        for (auto const& [b, bucket] : hipony::enumerate(bucketgroup)) {
          for (auto const& [e, entry] : hipony::enumerate(bucket)) {
            if (entry == target_entry) {
              auto const& bg =
                  *reinterpret_cast<BucketGroup*>(remote_read_address[g]);
              return reinterpret_cast<uintptr_t>(&(bg[b][e]));
            }
          }
        }
      }
      return std::nullopt;
    }
  };

  IndexFuture makeFuture(uint64_t id) {
    return IndexFuture(shared_from_this(), id);
  }

  // Delete
  // class TryDeleteFuture: public BasicFuture<bool> {
  // private:
  //   enum Step {
  //     Searching,
  //     Deleting,
  //     DoneDeleted,
  //     DoneNotDeleted,
  //   };
  //   Step step = Step::Searching;
  //   BucketEntry to_delete;
  //   SearchFuture search_future;
  // public:
  //  TryDeleteFuture(std::shared_ptr<IndexClient> _client, HashedKey hkey,
  //                  Value const value)
  //      : BasicFuture(std::move(_client)),
  //        to_delete{checksum(hkey), value},
  //        search_future{this->client, hkey} {}

  //  bool isDone() const override {
  //    return step == Step::DoneDeleted || step == Step::DoneNotDeleted;
  //   }

  //   void tryStepForward() override {
  //     switch (step) {
  //       case Step::Searching: {
  //         search_future.tryStepForward();
  //         if (!search_future.isDone()) {
  //           return;
  //         }
  //         if (auto const op_address = search_future.getAddressOf(to_delete))
  //         {
  //           step = Deleting;
  //           // client->logger->info("RDMA-casing {} -> 0 at {}, storing at
  //           {}, wr_id={}",
  //           // to_delete.as_uint64(), *op_address, ctx.rdma_buffer,
  //           ctx_wr_id()); ctx.ongoing_rdma = 1;
  //           client->rc->postSendSingleCas(ctxWrId(),
  //           &ctx.getTryDeleteBuffer(),
  //                                         *op_address, to_delete.asUint64(),
  //                                         0);
  //           client->to_poll++;
  //         } else {
  //           step = Step::DoneNotDeleted;
  //         }
  //         break;
  //       }
  //       case Step::Deleting: {
  //         if (ctx.ongoing_rdma) {
  //           return;
  //         }
  //         if (ctx.getTryInsertBuffer() == to_delete) { // The CAS succeeded
  //           step = Step::DoneDeleted;
  //         } else {
  //           step = Step::DoneNotDeleted;
  //         }
  //         break;
  //       }
  //       default: {}
  //     }
  //   }

  //   bool get() const override {
  //     switch (step) {
  //       case DoneDeleted: return true;
  //       case DoneNotDeleted: return false;
  //       default: throw std::runtime_error("Unexpected step");
  //     }
  //   }
  // };

  // TryDeleteFuture tryDelete(HashedKey hkey, Value const value) {
  //   return TryDeleteFuture(shared_from_this(), hkey, value);
  // }
};

}  // namespace dory::race