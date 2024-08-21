#pragma once

#include <atomic>
#include <queue>
#include <string>
#include <optional>
#include <array>
#include <variant>
#include <memory>
#include <vector>
#include <set>
#include <stdexcept>
#include <mutex>

#include <hipony/enumerate.hpp>
#include <xxhash.h>
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
#include "constants.hpp"
#include "lru-cache.hpp"

namespace dory::race {

class Client: public std::enable_shared_from_this<Client> {
public:
  using KeyView = std::string_view;
  using Key = std::string;
  using HashedKey = std::array<uint8_t, 16>;
  using Value = dory::race::Value;

private:
  LOGGER_DECL_INIT(logger, "Client");
  size_t bucket_bits;

  std::mutex cache_mutex;
  LRUCache<uintptr_t, Bucket> cache;

  struct WrContext {
    std::atomic<bool> ongoing_rdma = false;
    void* rdma_buffer;

    WrContext(void* const rdma_buffer) : rdma_buffer{rdma_buffer} {}
    static WrContext& at(uint64_t const wr_id) {
      return *reinterpret_cast<WrContext*>(wr_id);
    }

    // Search
    using SearchBuffer = std::array<Bucket, 2>;
    SearchBuffer& getSearchBuffer() { return *reinterpret_cast<SearchBuffer*>(rdma_buffer); }

    // TryInsert
    using TryInsertBuffer = BucketEntry;
    TryInsertBuffer& getTryInsertBuffer() { return *reinterpret_cast<TryInsertBuffer*>(rdma_buffer); }

    // Delete
    using TryDeleteBuffer = BucketEntry;
    TryDeleteBuffer& getTryDeleteBuffer() { return *reinterpret_cast<TryDeleteBuffer*>(rdma_buffer); }

    static size_t constexpr BufferSize = std::max({sizeof(SearchBuffer), sizeof(TryInsertBuffer), sizeof(TryDeleteBuffer)});
  };

  std::mutex wr_ctxs_mutex;
  static size_t constexpr NbContexts = conn::ReliableConnection::WrDepth;
  std::deque<std::unique_ptr<WrContext>> free_wr_ctxs, almost_free_wr_ctxs;

  std::mutex wce_mutex;
  std::vector<struct ibv_wc> wce;
  std::atomic<size_t> to_poll = 0;

  std::unique_ptr<WrContext> alloc_wr_ctx() {
    std::lock_guard<std::mutex> lock(wr_ctxs_mutex);
    if (free_wr_ctxs.empty())
      throw std::runtime_error("Ran out of wr ctxs!");
    auto ret = std::move(free_wr_ctxs.back());
    free_wr_ctxs.pop_back();
    return ret;
  }

  void free_wr_ctx(std::unique_ptr<WrContext>&& ctx) {
    std::lock_guard<std::mutex> lock(wr_ctxs_mutex);
    if (ctx->ongoing_rdma) {
      almost_free_wr_ctxs.emplace_front(std::move(ctx));
    } else {
      free_wr_ctxs.emplace_back(std::move(ctx));
    }
  }

  static HashedKey hash(KeyView const& key) {
    auto const h = XXH3_128bits(key.data(), key.size());
    return *reinterpret_cast<HashedKey const*>(&h);
  }

  static BucketEntry::Checksum checksum(KeyView const& key) {
    return hash(key)[15];
  }

  size_t bucket_offsets(KeyView const& key) const {
    auto const h = hash(key);
    size_t const mask = (1 << bucket_bits) - 1;
    auto const masked = *reinterpret_cast<uint64_t const*>(h.data()) & mask;
    auto const bucket_idx = (masked >> 1) * 3 + (masked & 1);
    return bucket_idx * sizeof(Bucket);
  }

  // RDMA
  ctrl::OpenDevice device;
  Delayed<ctrl::ControlBlock> cb;
  Delayed<conn::ReliableConnection> rc;

public:
  Client(ProcId const id, ProcId const server_id, size_t const bucket_bits, std::string const& name="default")
  : bucket_bits{bucket_bits},
    device{std::move(ctrl::Devices().list()[0])} {
  
    static size_t constexpr AllocatedSize = WrContext::BufferSize * NbContexts;

    ctrl::ResolvedPort port{device};
    if (!port.bindTo(0)) throw std::runtime_error("Couldn't bind the device.");
    cb.emplace(port);
    cb->registerPd("pd");
    cb->allocateBuffer("buf", AllocatedSize, 64);
    cb->registerMr(
        "mr", "pd", "buf",
        ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE | ctrl::ControlBlock::REMOTE_ATOMIC);
    cb->registerCq("cq");

    // 4. We establish reliable connections.
    auto& store = memstore::MemoryStore::getInstance();

    dory::conn::RcConnectionExchanger<ProcId> ce(id, {server_id}, *cb);
    auto const topic = fmt::format("{}-race-qp", name);
    ce.configureAll("pd", "mr", "cq", "cq");
    ce.announceAll(store, topic);
    ce.announceReady(store, topic, "prepared");
    logger->info("Connecting...");
    ce.waitReadyAll(store, topic, "prepared");
    ce.connectAll(
      store, topic,
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
      ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE | ctrl::ControlBlock::REMOTE_ATOMIC);
    logger->info("Connected!");
    logger->info("RDMA buffer: {} to {}", cb->mr("mr").addr, cb->mr("mr").addr + cb->mr("mr").size);
    rc.emplace(ce.extract(server_id));

    auto buffer = cb->mr("mr").addr;
    for (size_t i = 0; i < NbContexts; i++) {
      free_wr_ctxs.emplace_back(std::make_unique<WrContext>(reinterpret_cast<void*>(buffer)));
      buffer += WrContext::BufferSize;
    }
    ce.announceReady(store, topic, "connected");
    ce.waitReadyAll(store, topic, "connected");
  }

  template<typename T>
  class BasicFuture {
  public:
    BasicFuture(std::shared_ptr<Client> client): client{client}, ctx{this->client->alloc_wr_ctx()} {}
    BasicFuture(BasicFuture const&) = delete;
    BasicFuture& operator=(BasicFuture const&) = delete;
    BasicFuture(BasicFuture&&) = default;
    BasicFuture& operator=(BasicFuture&&) = default;

    virtual bool is_done() const = 0;
    virtual void move_forward() = 0;
    virtual T get() const = 0;
    virtual T await() {
      while (!is_done()) {
        client->tick_rdma();
        move_forward();
      }
      return get();
    }
  protected:
    std::shared_ptr<Client> client;
    std::unique_ptr<WrContext> ctx;
    MoveIndicator moved;

    uint64_t ctx_wr_id() {
      return reinterpret_cast<uint64_t>(ctx.get());
    }

    ~BasicFuture() {
      if (!moved) client->free_wr_ctx(std::move(ctx));
    }
  };

  void tick_rdma() {
    {
      std::lock_guard<std::mutex> lock(wce_mutex);
      wce.resize(to_poll);
      if (!rc->pollCqIsOk(conn::ReliableConnection::SendCq, wce))
        throw std::runtime_error("Error polling cq");
      for (auto const& wc : wce) {
        if (wc.status != IBV_WC_SUCCESS) {
          logger->error("RDMA WC status failure: {}", wc.status);
          throw std::runtime_error("WC unsuccessful.");
        }
        // logger->info("RDMA {} completed", wc.wr_id);
        WrContext::at(wc.wr_id).ongoing_rdma = false;
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
    std::array<std::optional<Value>, NbBucketEntries*2> matches;
    size_t nb_free = 0;
    std::array<std::optional<uintptr_t>, NbBucketEntries*2> free;
  };

  class SearchFuture: public BasicFuture<Matches> {
  private:
    enum Step {
      Searching,
      Done
    };
    Step step = Step::Searching;
    BucketEntry::Checksum checksum;
    uintptr_t remote_read_address;
  public:
    SearchFuture(std::shared_ptr<Client> client, KeyView const& key): BasicFuture{client}, checksum{Client::checksum(key)} {
      remote_read_address = client->rc->remoteBuf() + client->bucket_offsets(key);
      // client->logger->info("RDMA-reading {} bytes at {}, storing at {}, wr_id={}", sizeof(WrContext::SearchBuffer), remote_read_address, ctx->rdma_buffer, ctx_wr_id());
      ctx->ongoing_rdma = true;
      client->rc->postSendSingle(conn::ReliableConnection::RdmaRead, ctx_wr_id(), ctx->rdma_buffer, sizeof(WrContext::SearchBuffer), remote_read_address);
      client->to_poll++;
    }

    bool is_done() const override {
      return step == Step::Done;
    }
    void move_forward() override {
      switch (step) {
        case Step::Searching: {
          if (!ctx->ongoing_rdma) {
            step = Step::Done;
          }
          break;
        }
        default: {}
      }
    }
    Matches get() const override {
      auto& buckets = ctx->getSearchBuffer();
      Matches ret;
      for (auto const& [b, bucket] : hipony::enumerate(buckets)) {
        for (auto const& [e, entry] : hipony::enumerate(bucket)) {
          if (!entry.used) {
            auto const& sb = *reinterpret_cast<WrContext::SearchBuffer*>(remote_read_address);
            ret.free[ret.nb_free++] = reinterpret_cast<uintptr_t>(&(sb[b][e]));
          }
          else if (entry.checksum == checksum) {
            ret.matches[ret.nb_matches++] = entry.get_value();
          }
        }
      }
      return ret;
    }

    std::optional<uintptr_t> get_address_of(BucketEntry const& target_entry) {
      auto& buckets = ctx->getSearchBuffer();
      for (auto const& [b, bucket] : hipony::enumerate(buckets)) {
        for (auto const& [e, entry] : hipony::enumerate(bucket)) {
          if (entry == target_entry) {
            auto const& sb = *reinterpret_cast<WrContext::SearchBuffer*>(remote_read_address);
            return reinterpret_cast<uintptr_t>(&(sb[b][e]));
          }
        }
      }
      return std::nullopt;
    }
  };

  SearchFuture search(KeyView const& key) {
    return SearchFuture(shared_from_this(), key);
  }

  // Try insert
  struct Ok {};
  using TryInsertResult = std::variant<Ok, Matches>;

  class TryInsertFuture: public BasicFuture<TryInsertResult> {
  private:
    enum Step {
      Searching,
      Inserting,
      DoneOk,
      DoneMatches
    };
    Step step = Step::Searching;
    Key key;
    // Value value;
    bool issued_cas;
    BucketEntry to_insert;
    std::set<Value> ignore;
    SearchFuture search_future;
  public:
    TryInsertFuture(std::shared_ptr<Client> client, Key const& key, Value const value, std::set<Value> const& ignore): BasicFuture{client}, key{key}, /*value{value},*/ to_insert{checksum(key), value}, ignore{ignore}, search_future{this->client, key} { }

    bool is_done() const override {
      return step == Step::DoneOk || step == Step::DoneMatches;
    }

    void move_forward() override {
      switch (step) {
        case Step::Searching: {
          search_future.move_forward();
          if (!search_future.is_done()) return;
          auto const matches = search_future.get();
          for (size_t i = 0; i < matches.nb_matches; i++) {
            if (ignore.find(*matches.matches[i]) == ignore.end()) {
              client->logger->warn("There was a non-ignored match during insertion");
              step = Step::DoneMatches;
              return;
            }
          }
          step = Step::Inserting;
          if (matches.nb_free == 0)
            throw std::runtime_error("Bucket ran out of space!!!");
          // client->logger->info("RDMA-casing 0 -> {} at {}, storing at {}, wr_id={}", to_insert.as_uint64(), *matches.free[0], ctx->rdma_buffer, ctx_wr_id());
          ctx->ongoing_rdma = true;
          client->rc->postSendSingleCas(ctx_wr_id(), &ctx->getTryInsertBuffer(), *matches.free[0], 0, to_insert.as_uint64());
          client->to_poll++;
          break;
        }
        case Step::Inserting: {
          if (ctx->ongoing_rdma) return;
          if (ctx->getTryInsertBuffer().as_uint64() == 0) { // The CAS succeeded
            step = Step::DoneOk;
          } else {
            // CAS failed: we should reissue the search
            // TODO: have a limit?
            step = Step::Searching;
            search_future = {this->client, key};
          }
          break;
        }
        default: {}
      }
    }

    TryInsertResult get() const override {
      TryInsertResult ret;
      auto const matches = search_future.get();
      if (matches.nb_matches != 0)
        return matches;
      return Ok{};
    }
  };

  TryInsertFuture try_insert(Key const& key, Value const value, std::set<Value> const& ignore) {
    return TryInsertFuture(shared_from_this(), key, value, ignore);
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
    Key key;
    BucketEntry to_delete;
    SearchFuture search_future;
  public:
    TryDeleteFuture(std::shared_ptr<Client> client, Key const& key, Value const value): BasicFuture(client), key{key}, to_delete{checksum(key), value}, search_future{this->client, key} { }

    bool is_done() const override {
      return step == Step::DoneDeleted || step == Step::DoneNotDeleted;
    }

    void move_forward() override {
      switch (step) {
        case Step::Searching: {
          search_future.move_forward();
          if (!search_future.is_done()) return;
          if (auto const op_address = search_future.get_address_of(to_delete)) {
            step = Deleting;
            // client->logger->info("RDMA-casing {} -> 0 at {}, storing at {}, wr_id={}", to_delete.as_uint64(), *op_address, ctx->rdma_buffer, ctx_wr_id());
            ctx->ongoing_rdma = true;
            client->rc->postSendSingleCas(ctx_wr_id(), &ctx->getTryDeleteBuffer(), *op_address, to_delete.as_uint64(), 0);
            client->to_poll++;
          } else {
            step = Step::DoneNotDeleted;
          }
          break;
        }
        case Step::Deleting: {
          if (ctx->ongoing_rdma) return;
          if (ctx->getTryInsertBuffer() == to_delete) { // The CAS succeeded
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

  TryDeleteFuture try_delete(Key const& key, Value const value) {
    return TryDeleteFuture(shared_from_this(), key, value);
  }
};

}