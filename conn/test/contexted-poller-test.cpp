#include <gtest/gtest.h>
#include <deque>
#include <limits>
#include <vector>

#include <dory/conn/contexted-poller.hpp>
#include <dory/conn/message-identifier.hpp>
#include <dory/conn/mocks/mocks.hpp>
#include <dory/extern/ibverbs.hpp>

using namespace dory;

using ProcId = unsigned;
using ReqId = unsigned;
using Packer = conn::Packer<mocks::MessageKind, ProcId, ReqId>;
using ContextedPoller = conn::ContextedPoller<Packer>;
using PollerManager = conn::PollerManager<Packer>;
using MessageKind = mocks::MessageKind;

static std::vector<ProcId> const remote_ids = {1, 2, 3};

TEST(PollerManager, ThreeKinds) {
  ibv_cq uninited_mock_cq;
  deleted_unique_ptr<struct ibv_cq> mock_cq(
      &uninited_mock_cq, [](ibv_cq * /*unused*/) noexcept {});
  PollerManager cp(mock_cq);
  cp.registerContext(MessageKind::KindA);
  cp.registerContext(MessageKind::KindB);
  cp.registerContext(MessageKind::KindC);
  cp.endRegistrations(3);

  std::deque<ibv_wc> entries(5);
  entries[0].wr_id = Packer::pack(MessageKind::KindA, 1, 1);
  entries[1].wr_id = Packer::pack(MessageKind::KindB, 2, 2);
  entries[2].wr_id = Packer::pack(MessageKind::KindC, 3, 3);
  entries[3].wr_id = Packer::pack(MessageKind::KindA, 4, 4);
  entries[4].wr_id = Packer::pack(MessageKind::KindB, 5, 5);

  mocks::Poller mp(entries, 0, true);

  {
    ContextedPoller &poller = cp.getPoller(MessageKind::KindA);
    std::vector<ibv_wc> my_entries(2);
    EXPECT_TRUE(poller(mock_cq, my_entries, mp));
    // Only 1 KindA will be found within the 2 first WC.
    EXPECT_EQ(1, my_entries.size());
    EXPECT_EQ(Packer::unpackKind(my_entries[0].wr_id), MessageKind::KindA);
    // The 3rd WC isn't KindA.
    EXPECT_TRUE(poller(mock_cq, my_entries, mp));
    EXPECT_EQ(0, my_entries.size());
    // 1 B and 1 C should have been put on their respective queues.
  }

  {
    ContextedPoller &poller = cp.getPoller(MessageKind::KindB);
    std::vector<ibv_wc> my_entries(3);
    EXPECT_TRUE(poller(mock_cq, my_entries, mp));
    EXPECT_EQ(2, my_entries.size());
    // The 1st B has already been polled and the second one will
    // be polled now (after polling a A which will be sent to its
    // queue).
    EXPECT_EQ(Packer::unpackKind(my_entries[0].wr_id), MessageKind::KindB);
    EXPECT_EQ(Packer::unpackKind(my_entries[1].wr_id), MessageKind::KindB);
    EXPECT_TRUE(poller(mock_cq, my_entries, mp));
    EXPECT_EQ(0, my_entries.size());
    // There are no more B.
  }

  {
    ContextedPoller &poller = cp.getPoller(MessageKind::KindC);
    std::vector<ibv_wc> my_entries(2);
    EXPECT_TRUE(poller(mock_cq, my_entries, mp));
    // Nothing should be actually polled but the C (WC 3) should
    // have been polled previously.
    EXPECT_EQ(1, my_entries.size());
    EXPECT_EQ(Packer::unpackKind(my_entries[0].wr_id), MessageKind::KindC);
    EXPECT_TRUE(poller(mock_cq, my_entries, mp));
    EXPECT_EQ(0, my_entries.size());
  }
}
