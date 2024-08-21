#pragma once

#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <dory/ctrl/block.hpp>
#include <dory/third-party/sync/spsc.hpp>

#include "message-identifier.hpp"

namespace dory::conn {
struct WC {
  uint64_t wr_id;             // Id of the completed Work Request (WR)
  enum ibv_wc_status status;  // Status of the operation

  WC() = default;
  WC(struct ibv_wc &wc) : wr_id{wc.wr_id}, status{wc.status} {}
};

struct DefaultPoller {};

template <typename Packer>
struct ContextedPoller {
  using KindType = typename Packer::KindType;
  using ProcIdType = typename Packer::ProcIdType;

  ContextedPoller(
      deleted_unique_ptr<struct ibv_cq> &cq, KindType context_kind,
      std::vector<dory::third_party::sync::SpscQueue<WC> *> &from,
      std::map<KindType, dory::third_party::sync::SpscQueue<WC> *> &to)
      : cq{cq}, context_kind{context_kind}, from{from}, to{to} {}

  // A contexted poller can be moved, but cannot be copied.
  ContextedPoller &operator=(ContextedPoller const &) = delete;
  ContextedPoller &operator=(ContextedPoller &&) = delete;
  ContextedPoller(ContextedPoller const &) = delete;
  ContextedPoller(ContextedPoller &&) noexcept = default;

  template <typename Poller = DefaultPoller>
  bool operator()(deleted_unique_ptr<struct ibv_cq> & /*unused*/,
                  std::vector<struct ibv_wc> &entries,
                  Poller &poller = default_poller) {
    return this->operator()(entries, poller);
  }

  template <typename Poller = DefaultPoller>
  bool operator()(std::vector<struct ibv_wc> &entries,
                  Poller &poller = default_poller) {
    auto num_requested = entries.size();
    size_t index = 0;

    // Go over all the queues and try to fulfill the request
    for (auto *queue : from) {
      WC returned;

      while (num_requested > 0 && queue->try_dequeue(returned)) {
        entries[index].wr_id = returned.wr_id;
        entries[index].status = returned.status;
        index += 1;
        num_requested -= 1;
      }

      if (num_requested == 0) {
        return true;
      }
    }

    // Poll the rest and distribute if necessary
    // note: if it returns an empty value, polling failed.
    auto const polled = [&]() -> std::optional<size_t> {
      if constexpr (std::is_same_v<Poller, DefaultPoller>) {
        auto const ret = ibv_poll_cq(cq.get(), static_cast<int>(num_requested),
                                     &entries[index]);
        if (ret >= 0) {
          return static_cast<size_t>(ret);
        }
        return {};
      } else {
        // We poll the WCs by using yet another proxy poller.
        std::vector<struct ibv_wc> proxied(num_requested);
        auto ok = poller(cq.get(), proxied);
        assert(entries.size() - index >= proxied.size());
        if (ok) {
          std::copy(proxied.cbegin(), proxied.cend(),
                    entries.begin() + static_cast<ssize_t>(index));
          return proxied.size();
        }
        return {};
      }
    }();

    if (polled) {
      auto const frozen_index = index;
      // The ones that are not ours, put them in their respective queues
      for (size_t i = frozen_index; i < frozen_index + *polled; i++) {
        auto &entry = entries[i];
        auto kind = Packer::unpackKind(entry.wr_id);

        if (kind == context_kind) {
          entries[index] = entry;
          index++;
        } else {
          auto queue = to.find(kind);
          if (queue == to.end()) {
            throw std::runtime_error(
                "No queue exists with kind " + std::string(kind.toStr()) +
                " (no " + std::to_string(kind.value) + ") while " + "polling " +
                std::string(context_kind.toStr()));
          }
          if (!queue->second->try_enqueue(WC{entry})) {
            // throw std::runtime_error("Queue overflowed");
            return false;
          }
        }
      }
    }

    entries.erase(entries.begin() + static_cast<ssize_t>(index), entries.end());

    return polled.has_value();
  }
  static DefaultPoller default_poller;

 private:
  deleted_unique_ptr<struct ibv_cq> &cq;
  KindType context_kind;
  std::vector<dory::third_party::sync::SpscQueue<WC> *> from;
  std::map<KindType, dory::third_party::sync::SpscQueue<WC> *> to;
};

template <typename P>
DefaultPoller ContextedPoller<P>::default_poller = {};

template <typename Packer>
class PollerManager {
 public:
  using KindType = typename Packer::KindType;
  using ProcIdType = typename Packer::ProcIdType;
  using SContextedPoller = ContextedPoller<Packer>;

  PollerManager(deleted_unique_ptr<struct ibv_cq> &cq) : cq{cq}, done{false} {}

  void registerContext(KindType context_kind) {
    std::lock_guard<std::mutex> const lock(contexts_mutex);
    if (contexts.find(context_kind) != contexts.end()) {
      throw std::runtime_error("Already registered polling context with id " +
                               std::string(context_kind.toStr()));
    }

    contexts.insert(context_kind);
  }

  void endRegistrations(size_t expected_nr_contexts) {
    while (true) {
      std::lock_guard<std::mutex> const lock(contexts_mutex);
      if (contexts.size() == expected_nr_contexts) {
        break;
      }
    }

    std::lock_guard<std::mutex> const lock(contexts_mutex);
    if (done.load()) {
      return;
    }

    done.store(true);

    // Create all to all queues
    for (auto cid_from : contexts) {
      for (auto cid_to : contexts) {
        if (cid_from == cid_to) {
          continue;
        }
        queues.insert(
            std::make_pair(std::make_pair(cid_from, cid_to),
                           dory::third_party::sync::SpscQueue<WC>(QueueDepth)));
      }
    }
  }

  // Todo: we could give proper ownership be moving the poller instead of giving
  // a reference to it. This would require some refactor.
  SContextedPoller &getPoller(KindType context_kind) {
    if (!done.load()) {
      throw std::runtime_error("PollerManager is not finalized");
    }

    if (contexts.find(context_kind) == contexts.end()) {
      throw std::runtime_error("This context kind was never registered.");
    }

    // There should be only ONE polling context for each kind.
    std::lock_guard<std::mutex> const lock(pollers_mutex);

    if (pollers.find(context_kind) == pollers.end()) {
      std::map<KindType, dory::third_party::sync::SpscQueue<WC> *> to_mapping;
      std::vector<dory::third_party::sync::SpscQueue<WC> *> from_list;
      // Create a compact map
      for (auto &[cid_from_to, queue] : queues) {
        auto from = cid_from_to.first;
        auto to = cid_from_to.second;
        if (context_kind == to) {
          from_list.push_back(&queue);
        }

        if (context_kind == from) {
          to_mapping.insert(std::make_pair(to, &queue));
        }
      }
      pollers.emplace(context_kind, SContextedPoller{cq, context_kind,
                                                     from_list, to_mapping});
    }

    return pollers.find(context_kind)->second;
  }

 private:
  deleted_unique_ptr<struct ibv_cq> &cq;
  std::map<std::pair<KindType, KindType>,
           dory::third_party::sync::SpscQueue<WC>>
      queues;  // (from, to) -> queue
  std::set<KindType> contexts;
  std::mutex contexts_mutex;
  std::atomic<bool> done;
  std::map<KindType, SContextedPoller> pollers;
  std::mutex pollers_mutex;

  static int constexpr QueueDepth = 1024;
};

}  // namespace dory::conn
