#pragma once

#include <array>
#include <atomic>
#include <optional>
#include <queue>
#include <string>
#include <utility>

#include <variant>
#include <memory>
#include <vector>
#include <set>
#include <stdexcept>
#include <mutex>

#include <hipony/enumerate.hpp>
#include <fmt/core.h>

#include <dory/ctrl/device.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/extern/ibverbs.hpp>
#include <dory/shared/types.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/move-indicator.hpp>

#include "types.hpp"

namespace dory::race {

class ClientIndex: public std::enable_shared_from_this<ClientIndex> {
public:
  using Value = dory::race::Value;

private:
  LOGGER_DECL_INIT(logger, "ClientIndex");
  size_t bucket_bits;

  struct WrContext {
    std::atomic<uint8_t> ongoing_rdma = 0;
    uint8_t* rdma_buffer;

    WrContext(uint8_t* const rdma_buffer) : rdma_buffer{rdma_buffer} {}
    static WrContext& at(uint64_t const wr_id) {
      return *reinterpret_cast<WrContext*>(wr_id);
    }

    // Search
    using SearchBuffer = std::array<BucketGroup, 2>;
    SearchBuffer& getSearchBuffer() const {
      return *reinterpret_cast<SearchBuffer*>(rdma_buffer);
    }

    // TryInsert
    using TryInsertBuffer = BucketEntry;
    TryInsertBuffer& getTryInsertBuffer() const {
      return *reinterpret_cast<TryInsertBuffer*>(rdma_buffer);
    }

    // Delete
    using TryDeleteBuffer = BucketEntry;
    TryDeleteBuffer& getTryDeleteBuffer() const {
      return *reinterpret_cast<TryDeleteBuffer*>(rdma_buffer);
    }

    static size_t constexpr BufferSize = std::max({sizeof(SearchBuffer), sizeof(TryInsertBuffer), sizeof(TryDeleteBuffer)});
  };

  std::mutex wr_ctxs_mutex;
  static size_t constexpr NbContexts = conn::ReliableConnection::WrDepth;
  std::deque<std::unique_ptr<WrContext>> free_wr_ctxs, almost_free_wr_ctxs;

  std::mutex wce_mutex;
  std::vector<struct ibv_wc> wce;
  std::atomic<size_t> to_poll = 0;

  std::unique_ptr<WrContext> allocWrCtx() {
    std::lock_guard<std::mutex> lock(wr_ctxs_mutex);
    if (free_wr_ctxs.empty()) {
      throw std::runtime_error("Ran out of wr ctxs!");
    }
    auto ret = std::move(free_wr_ctxs.back());
    free_wr_ctxs.pop_back();
    return ret;
  }

  void freeWrCtx(std::unique_ptr<WrContext>&& ctx) {
    std::lock_guard<std::mutex> lock(wr_ctxs_mutex);
    if (ctx->ongoing_rdma) {
      almost_free_wr_ctxs.emplace_front(std::move(ctx));
    } else {
      free_wr_ctxs.emplace_back(std::move(ctx));
    }
  }

  static BucketEntry::Checksum checksum(HashedKey const& hkey) {
    return hkey[15];
  }

  std::array<uint64_t, 2> bucketOffsets(HashedKey const& hkey) const {
    size_t const mask = (1 << bucket_bits) - 1;
    std::array<uint64_t, 2> bucket_ids;
    bucket_ids[0] = reinterpret_cast<uint64_t const*>(hkey.data())[0] & mask;
    bucket_ids[1] = reinterpret_cast<uint64_t const*>(hkey.data())[1] % (mask - 1);
    if ((bucket_ids[1] >> 1) >= (bucket_ids[0] >> 1)) {
      bucket_ids[1] += 2;
    }
    assert(bucket_ids[0] != bucket_ids[1]);
    assert((bucket_ids[1] & mask) == bucket_ids[1]);
    return {bucket_ids[0], bucket_ids[1]};
  }

  // RDMA
  ctrl::OpenDevice device;
  Delayed<ctrl::ControlBlock> cb;
  Delayed<conn::ReliableConnection> rc;

public:
  ClientIndex(ProcId const id, ProcId const server_id, size_t const bucket_bits, std::string const& name="default")
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
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE | ctrl::ControlBlock::REMOTE_ATOMIC);
    cb->registerCq("index-cq");

    // 4. We establish reliable connections.
    auto& store = memstore::MemoryStore::getInstance();

    dory::conn::RcConnectionExchanger<ProcId> ce(id, {server_id}, *cb);
    auto const topic = fmt::format("{}-race-qp", name);
    ce.configureAll("pd", "mr", "index-cq", "index-cq");
    ce.announceAll(store, topic);
    ce.unannounceReady(store, topic, "connected"); // Clean up from previous runs
    ce.announceReady(store, topic, "prepared");
    logger->info("Connecting...");
    ce.waitReadyAll(store, topic, "prepared");
    ce.connectAll(
      store, topic,
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
      ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE | ctrl::ControlBlock::REMOTE_ATOMIC);
    logger->info("Connected!");
    logger->info("RDMA buffer: {} to {}", cb->mr("mr").addr, cb->mr("mr").addr + cb->mr("mr").size);

    auto buffer = cb->mr("mr").addr;
    for (size_t i = 0; i < NbContexts; i++) {
      free_wr_ctxs.emplace_back(std::make_unique<WrContext>(reinterpret_cast<uint8_t*>(buffer)));
      buffer += WrContext::BufferSize;
    }
    ce.announceReady(store, topic, "connected");
    ce.waitReadyAll(store, topic, "connected");
    // Clean up for the next run.
    ce.unannounceAll(store, topic);
    ce.unannounceReady(store, topic, "prepared");
    rc.emplace(ce.extract(server_id));
  }

  template<typename T>
  class BasicFuture {
  public:
    BasicFuture(std::shared_ptr<ClientIndex> _client): client{std::move(_client)}, ctx{this->client->allocWrCtx()} {}
    BasicFuture(BasicFuture const&) = delete;
    BasicFuture& operator=(BasicFuture const&) = delete;
    BasicFuture(BasicFuture&&)  noexcept = default;
    BasicFuture& operator=(BasicFuture&&)  noexcept = default;

    virtual bool isDone() const = 0;
    virtual void moveForward() = 0;
    virtual T get() const = 0;
    virtual T await() {
      while (!isDone()) {
        client->tickRdma();
        moveForward();
      }
      return get();
    }
  protected:
    std::shared_ptr<ClientIndex> client;
    std::unique_ptr<WrContext> ctx;
    MoveIndicator moved;

    uint64_t ctxWrId() {
      return reinterpret_cast<uint64_t>(ctx.get());
    }

    virtual ~BasicFuture() {
      if (!moved) {
        client->freeWrCtx(std::move(ctx));
      }
    }
  };

  void tickRdma() {
    {
      std::lock_guard<std::mutex> lock(wce_mutex);
      wce.resize(to_poll);
      if (!rc->pollCqIsOk(conn::ReliableConnection::SendCq, wce)) {
        throw std::runtime_error("Error polling cq");
      }
      for (auto const& wc : wce) {
        if (wc.status != IBV_WC_SUCCESS) {
          logger->error("RDMA WC status failure: {}", wc.status);
          throw std::runtime_error("WC unsuccessful.");
        }
        // logger->info("RDMA {} completed", wc.wr_id);
        WrContext::at(wc.wr_id).ongoing_rdma -= 1;
      }
      to_poll -= wce.size();
    }
    {
      std::lock_guard<std::mutex> lock(wr_ctxs_mutex);
      while (!almost_free_wr_ctxs.empty() && !almost_free_wr_ctxs.back()->ongoing_rdma) {
        auto& to_move = almost_free_wr_ctxs.back();
        free_wr_ctxs.emplace_back(std::move(to_move));
        almost_free_wr_ctxs.pop_back();
      }
    }
  }

  // Search
  struct Matches {
    size_t nb_matches = 0;
    std::array<BucketEntry, NbBucketEntries*2*2> matches;
    size_t nb_free = 0;
    uintptr_t free;
  };

  class SearchFuture: public BasicFuture<Matches> {
  private:
    enum Step {
      Searching,
      Done
    };
    Step step = Step::Searching;
    uint64_t remote_read_offsets[2];
    bool bucket_side[2];

    struct ibv_send_wr wr[2];
    struct ibv_sge sg[2];
  public:
    HashedKey hkey;

    SearchFuture(const std::shared_ptr<ClientIndex> _client,
                 HashedKey hkey)
        : BasicFuture{std::move(_client)}, hkey{hkey} {
      auto const bucket_ids = client->bucketOffsets(hkey);
      auto& bucketgroups = ctx->getSearchBuffer();

      for(auto const group : {0u, 1u}) {
        bucket_side[group] = (bucket_ids[group] & 1) == 1;
        auto const bucket_location = (bucket_ids[group] >> 1) * 3 + bucket_side[group];
        remote_read_offsets[group] = bucket_location * sizeof(Bucket);
      }

      ctx->ongoing_rdma = 1;
      client->to_poll += 1;
      for(auto const group : {0u, 1u}) {
        client->rc->prepareSingle(wr[group], sg[group],
                                  conn::ReliableConnection::RdmaRead, ctxWrId(),
                                  &(bucketgroups[group]), sizeof(BucketGroup),
                                  remote_read_offsets[group] + client->rc->remoteBuf(),
                                  group == 1);
        client->rc->postSend(wr[group]);
      }
      // wr[0].next = &wr[1];
      // client->rc->postSend(wr[0]);
    }

    bool isDone() const override {
      return step == Step::Done;
    }

    void moveForward() override {
      switch (step) {
        case Step::Searching: {
          if (!ctx->ongoing_rdma) {
            step = Step::Done;

            // TODO(zyf): also cache free slots as bytes (insert optimization)
          }
          break;
        }
        default: {}
      }
    }

    Matches get() const override {
      auto& bucketgroups = ctx->getSearchBuffer();
      Matches ret;
      uintptr_t free[2] = {0UL, 0UL};
      uint32_t group_free_nb[2] = {0U, 0U};

      // Traverse non-overflow buckets first
      for (auto const is_overflow: {false, true}) {
        for (auto const& [g, bucketgroup] : hipony::enumerate(bucketgroups)) {
          bool b = (bucket_side[g] != is_overflow); // XOR
          auto bucket = bucketgroup[b];

          for (auto const& [e, entry] : hipony::enumerate(bucket)) {
            if (!entry.used) {
              if(group_free_nb[g]++ == 0) {
                auto const& bg = *reinterpret_cast<BucketGroup*>(remote_read_offsets[g]);
                free[g] = reinterpret_cast<uintptr_t>(&(bg[b][e]));
              }
            } else if (entry.checksum == checksum(hkey)) {
              ret.matches[ret.nb_matches++] = entry;
            }
          }
        }
      }

      ret.nb_free = group_free_nb[0] + group_free_nb[1];
      // client->logger->info("Free slots: {}+{}", group_free_nb[0], group_free_nb[1]);
      // Return the first free slot in the least full bucketgroup
      ret.free = free[group_free_nb[0] < group_free_nb[1]];
      return ret;
    }

    std::optional<uintptr_t> getOffsetOf(BucketEntry const& target_entry) {
      auto& bucketgroups = ctx->getSearchBuffer();
      for (auto const& [g, bucketgroup] : hipony::enumerate(bucketgroups)) {
        for (auto const& [b, bucket] : hipony::enumerate(bucketgroup)) {
          for (auto const& [e, entry] : hipony::enumerate(bucket)) {
            if (entry == target_entry) {
              auto const& bg = *reinterpret_cast<BucketGroup*>(remote_read_offsets[g]);
              return reinterpret_cast<uintptr_t>(&(bg[b][e]));
            }
          }
        }
      }
      return std::nullopt;
    }

    BucketEntry entryFor(const Value value) {
      return BucketEntry{checksum(hkey), value};
    }
  };

  SearchFuture search(HashedKey hkey) {
    return SearchFuture(shared_from_this(), hkey);
  }

  // Try insert
  using TryInsertResult = BucketEntry;

  class TryInsertFuture: public BasicFuture<TryInsertResult> {
  private:
    enum Step {
      Inserting,
      Done
    };
    Step step = Step::Inserting;
  public:
    TryInsertFuture(std::shared_ptr<ClientIndex> _client, uintptr_t slot_offset, uint64_t expected, HashedKey hkey, 
                    Value const value)
      : BasicFuture{std::move(_client)} {
      auto const to_insert = BucketEntry{checksum(hkey), value};

      // client->logger->info("RDMA-casing 0 -> {} at {}, wr_id={}", to_insert.as_uint64(),
      // matches.free, ctx_wr_id());
      ctx->ongoing_rdma = 1;
      client->rc->postSendSingleCas(ctxWrId(), &ctx->getTryInsertBuffer(),
                                    slot_offset + client->rc->remoteBuf(),
                                    expected, to_insert.asUint64());
      client->to_poll++;
    }
    
  
    // Fake "TryInsertFuture" that will simply read a single slot
    TryInsertFuture(std::shared_ptr<ClientIndex> _client, uintptr_t slot_offset)
        : BasicFuture{std::move(_client)} {
      ctx->ongoing_rdma = 1;
      client->rc->postSendSingle(conn::ReliableConnection::RdmaRead, ctxWrId(),
                                &ctx->getTryInsertBuffer(), sizeof(BucketEntry),
                                slot_offset + client->rc->remoteBuf());
      client->to_poll++;
    }

    bool isDone() const override {
     return step == Step::Done;
    }

    void moveForward() override {
      switch (step) {
        case Step::Inserting: {
          if (ctx->ongoing_rdma) {
            return;
          }
          step = Step::Done;
          break;
        }
        default: {}
      }
    }

    TryInsertResult get() const override {
      return ctx->getTryInsertBuffer();
    }
  };

  TryInsertFuture tryInsert(uintptr_t slot_offset, HashedKey hkey, Value const value) {
    return TryInsertFuture(shared_from_this(), slot_offset, 0, hkey, value);
  }

  TryInsertFuture tryUpdate(uintptr_t slot_offset, BucketEntry const previous_entry, HashedKey hkey, Value const value) {
    return TryInsertFuture(shared_from_this(), slot_offset, previous_entry.asUint64(), hkey, value);
  }

  TryInsertFuture tryCheck(uintptr_t slot_offset) {
    return TryInsertFuture(shared_from_this(), slot_offset);
  }

  // Delete
  class TryDeleteFuture: public BasicFuture<bool> {
  private:
    enum Step {
      Searching,
      Deleting,
      DoneDeleted,
      DoneNotDeleted,
    };
    Step step = Step::Searching;
    BucketEntry to_delete;
    SearchFuture search_future;
  public:
   TryDeleteFuture(std::shared_ptr<ClientIndex> _client, HashedKey hkey,
                   Value const value)
       : BasicFuture(std::move(_client)),
         to_delete{checksum(hkey), value},
         search_future{this->client, hkey} {}

   bool isDone() const override {
     return step == Step::DoneDeleted || step == Step::DoneNotDeleted;
    }

    void moveForward() override {
      switch (step) {
        case Step::Searching: {
          search_future.moveForward();
          if (!search_future.isDone()) {
            return;
          }
          if (auto const slot_offset = search_future.getOffsetOf(to_delete)) {
            step = Deleting;
            // client->logger->info("RDMA-casing {} -> 0 at {}, storing at {}, wr_id={}",
            // to_delete.as_uint64(), *op_address, ctx->rdma_buffer, ctx_wr_id());
            ctx->ongoing_rdma = 1;
            client->rc->postSendSingleCas(ctxWrId(), &ctx->getTryDeleteBuffer(),
                                          *slot_offset + client->rc->remoteBuf(),
                                          to_delete.asUint64(), 0);
            client->to_poll++;
          } else {
            step = Step::DoneNotDeleted;
          }
          break;
        }
        case Step::Deleting: {
          if (ctx->ongoing_rdma) {
            return;
          }
          if (ctx->getTryDeleteBuffer() == to_delete) { // The CAS succeeded
            step = Step::DoneDeleted;
          } else {
            step = Step::DoneNotDeleted;
          }
          break;
        }
        default: {}
      }
    }

    bool get() const override {
      switch (step) {
        case DoneDeleted: return true;
        case DoneNotDeleted: return false;
        default: throw std::runtime_error("Unexpected step");
      }
    }
  };

  TryDeleteFuture tryDelete(HashedKey hkey, Value const value) {
    return TryDeleteFuture(shared_from_this(), hkey, value);
  }
};

}  // namespace dory::race