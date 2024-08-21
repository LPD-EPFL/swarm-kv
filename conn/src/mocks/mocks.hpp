#pragma once

#include <deque>
#include <map>
#include <utility>

#include <vector>

#include <dory/extern/ibverbs.hpp>

#include "../contexted-poller.hpp"
#include "../message-identifier.hpp"
#include "../rc.hpp"

namespace mocks {

class MessageKind : public dory::conn::BaseKind<MessageKind, uint64_t> {
 public:
  enum Value : uint64_t {
    KindA = 1,
    KindB = 2,
    KindC = 3,
    MAX_KIND_VALUE__ = 3
  };

  constexpr MessageKind(Value v) { value = v; }

  constexpr char const* toStr() const {
    switch (value) {
      case KindA:
        return "MessageKind::KindA";
      case KindB:
        return "MessageKind::KindB";
      case KindC:
        return "MessageKind::KindC";
      default:
        return "Out of range";
    }
  }
};
}  // namespace mocks

namespace mocks {

template <typename Packer>
class ConnectionContext {
 public:
  using KindType = typename Packer::KindType;
  using ProcIdType = typename Packer::ProcIdType;
  class RcConnectionExchanger {
   public:
    std::map<int, dory::conn::ReliableConnection>& connections() {
      return empty_map;
    }
    std::map<int, dory::conn::ReliableConnection> empty_map;
  };
  class CompletionQueue {
   public:
    ibv_cq* get() { return nullptr; }
  };
  class ControlBlock {
   public:
    bool pollCqIsOk(CompletionQueue /*unused*/,
                    std::vector<ProcIdType> /*unused*/) {
      return true;
    }
  };
  class PollerManager {
   public:
    dory::conn::ContextedPoller<Packer>& getPoller(KindType& /*unused*/) {
      // Warn: THIS IS UB BUT SHOULD BE Ok FOR THE MOCK AS IT WILL NEVER BE
      //       CALLED;
      dory::conn::ContextedPoller<Packer>* cpp = nullptr;
      return *cpp;
    }
  };
  static std::vector<int> empty_vec;
  RcConnectionExchanger ce;
  ControlBlock cb;
  CompletionQueue cq;
  PollerManager poller_manager;
};

class Poller {
 public:
  Poller(std::optional<std::deque<ibv_wc>> entries, int latency = 0,
         bool bounded = false)
      : entries{std::move(entries)}, latency{latency}, bounded{bounded} {}
  template <typename Cq>
  bool operator()(Cq /*unused*/, std::vector<ibv_wc>& output) {
    // The poller won't produce any WC before being called latency times.
    if (latency-- <= 0) {
      // If no entries are given, it will return an error.
      if (entries) {
        // It will try to put as many WC in the output as given and holdable
        // unless unbounded.
        auto requested = output.size();
        output.clear();
        for (auto inserted = 0UL;
             !entries->empty() && (!bounded || inserted < requested);
             inserted++) {
          output.push_back((*entries)[0]);
          entries->pop_front();
        }
        return true;
      }
      return false;
    }  // The latency critera isn't met so we poll nothing.

    output.resize(0);

    return true;
  }
  std::optional<std::deque<ibv_wc>> entries;
  int latency;
  bool bounded;
};
}  // namespace mocks
