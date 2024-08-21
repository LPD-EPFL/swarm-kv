#pragma once

#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>

#include <arpa/inet.h>
#include <netinet/in.h>

#include <dory/ctrl/block.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/move-indicator.hpp>
#include <utility>

namespace dory::conn {
struct UnreliableDatagramInfo {
  UnreliableDatagramInfo() : UnreliableDatagramInfo(0, 0, 0) {}

  UnreliableDatagramInfo(uint16_t lid, uint32_t qpn, uint32_t qkey)
      : lid{lid}, qpn{qpn}, qkey{qkey} {}

  std::string serialize() const {
    std::ostringstream os;

    os << std::hex << lid << "/" << qpn << "/" << qkey;
    return os.str();
  }

  static UnreliableDatagramInfo fromSerialized(std::string const &serialized) {
    std::string res(serialized);
    std::replace(res.begin(), res.end(), '/', ' ');  // replace '/' by ' '

    UnreliableDatagramInfo ud_info;

    std::stringstream ss(res);
    ss >> std::hex >> ud_info.lid;
    ss >> std::hex >> ud_info.qpn;
    ss >> std::hex >> ud_info.qkey;

    return ud_info;
  }

  uint16_t lid;
  uint32_t qpn;
  uint32_t qkey;
};
}  // namespace dory::conn

namespace dory::conn {
struct McGroupDatagramInfo {
  McGroupDatagramInfo() : McGroupDatagramInfo({}, 0) {}

  McGroupDatagramInfo(union ibv_gid const &gid, uint16_t lid)
      : gid{gid}, lid{lid} {}

  std::string serialize() const {
    char ipv6_buf[INET6_ADDRSTRLEN + 1];

    auto const *ipv6_str =
        inet_ntop(AF_INET6, gid.raw, ipv6_buf, INET6_ADDRSTRLEN);
    if (ipv6_str == nullptr) {
      throw std::runtime_error("Could not convert the GId to string: " +
                               std::string(strerror(errno)));
    }

    std::ostringstream os;
    os << ipv6_str << "/";
    os << std::hex << lid;
    return os.str();
  }

  static McGroupDatagramInfo fromSerialized(std::string const &serialized) {
    McGroupDatagramInfo mcud_info;

    std::string res(serialized);
    std::replace(res.begin(), res.end(), '/', ' ');  // replace '/' by ' '

    std::stringstream ss(res);

    std::string ipv6_gid;
    ss >> ipv6_gid;

    int rc = inet_pton(AF_INET6, ipv6_gid.c_str(), mcud_info.gid.raw);
    switch (rc) {
      case 0:
        throw std::runtime_error(
            "The provided string does not contain a valid GId address");
        break;
      case -1:
        throw std::runtime_error("Not supported address family: " +
                                 std::string(strerror(errno)));
        break;
      case 1:
        // Success
        break;
      default:
        throw std::runtime_error("Unreachable");
    }

    ss >> std::hex >> mcud_info.lid;

    return mcud_info;
  }

  union ibv_gid gid;
  uint16_t lid;
};
}  // namespace dory::conn

namespace dory::conn {
template <size_t MaxBatchSz = 32>
class UdBatch {
 public:
  static size_t constexpr MaxSize = MaxBatchSz;

  UdBatch() = default;

  UdBatch(UdBatch &&) = delete;
  UdBatch &operator=(UdBatch const &) = delete;
  UdBatch(UdBatch const &) = delete;
  UdBatch &operator=(UdBatch &&) = delete;

  UdBatch &append(struct ibv_ah *ah, uint64_t req_id, void *buf, uint32_t len,
                  uint32_t immediate,
                  ctrl::ControlBlock::MemoryRegion const &mr, uint32_t qpn,
                  uint32_t qkey);

  struct ibv_send_wr &head();

  size_t size() const { return i; }

  void reset() { i = 0; }

 private:
  struct Request {
    struct ibv_sge sg;
    struct ibv_send_wr wr;
  };

  size_t i = 0;
  std::array<Request, MaxBatchSz> requests;
};

/**
 * @brief Wrapper around a local UD QP.
 *
 */
class UnreliableDatagram {
 public:
  enum Cq { SendCQ, RecvCQ };

  static int constexpr WrDepth = 128;
  static int constexpr SgeDepth = 16;
  static int constexpr MaxInlining = 256;
  static uint32_t constexpr DefaultPsn = 0;
  static uint32_t constexpr DefaultQKey = 0;
  static uint32_t constexpr McQpn = 0xFFFFFF;
  static int constexpr UdGrhLength = 40;

  /**
   * @brief Construct a new Unreliable Datagram object.
   *
   * The QP is in RTR state.
   *
   * @param cb
   * @param pd_name
   * @param mr_name
   * @param send_cq_name
   * @param recv_cq_name
   */
  UnreliableDatagram(ctrl::ControlBlock &cb, std::string const &pd_name,
                     std::string const &mr_name,
                     std::string const &send_cq_name,
                     std::string const &recv_cq_name);

  // A UD can be moved, but not copied (it uses std::unique_ptr).
  UnreliableDatagram(UnreliableDatagram &&) = default;
  UnreliableDatagram &operator=(UnreliableDatagram const &) = delete;
  UnreliableDatagram(UnreliableDatagram const &) = delete;
  // Deleted as we don't use it.
  UnreliableDatagram &operator=(UnreliableDatagram &&) = delete;

  /**
   * @brief Returns the serialized information of the underlying UD QP so that
   *        it can be contacted remotely.
   *
   * '/' are used as separators as ':' won't be available if the data contains
   * an ipv6 address and for consistency.
   *
   * @return std::string
   */
  UnreliableDatagramInfo info() const;

  /**
   * @brief Polls one of the Cq and return whether or not is succeed.
   *
   * @tparam cq Which Cq to poll
   * @param entries
   * @return true on polling success (even if no entries are polled).
   * @return false on polling error.
   */
  template <Cq cq>
  bool pollCqIsOk(std::vector<struct ibv_wc> &entries) {
    int num = 0;

    static_assert(cq == RecvCQ || cq == SendCQ, "Invalid Cq type");

    if constexpr (cq == RecvCQ) {
      num = ibv_poll_cq(recv_cq, static_cast<int>(entries.size()), &entries[0]);
    } else {
      num = ibv_poll_cq(send_cq, static_cast<int>(entries.size()), &entries[0]);
    }

    if (num >= 0) {
      entries.erase(entries.begin() + num, entries.end());
      return true;
    }
    return false;
  }

  template <size_t MaxBatchSz>
  void append(UdBatch<MaxBatchSz> &batch, struct ibv_ah *ah, uint64_t req_id,
              void *buf, uint32_t len, uint32_t immediate, uint32_t qpn,
              uint32_t qkey) {
    batch.append(ah, req_id, buf, len, immediate, mr, qpn, qkey);
  }

  template <size_t MaxBatchSz>
  bool postSend(UdBatch<MaxBatchSz> &batch) {
    return postSend(batch.head());
  }

  /**
   * @brief Posts a send request to a given remote UD QP.
   *
   * The buffer must lie within the MR given at construction time.
   *
   * @param req_id
   * @param buf
   * @param len
   * @param ah
   * @param qpn
   * @param qkey
   * @return true
   * @return false
   */
  bool postSend(uint64_t req_id, void *buf, uint32_t len, struct ibv_ah *ah,
                uint32_t qpn, uint32_t qkey);

  /**
   * @brief Posts a send request with immediate to a given remote UD QP.
   *
   * The buffer must lie within the MR given at construction time.
   *
   * @param req_id
   * @param buf
   * @param len
   * @param immediate
   * @param ah
   * @param qpn
   * @param qkey
   * @return true
   * @return false
   */
  bool postSend(uint64_t req_id, void *buf, uint32_t len, uint32_t immediate,
                struct ibv_ah *ah, uint32_t qpn, uint32_t qkey);

  /**
   * @brief Posts a recv request.
   *
   * The buffer must lie within the MR given at construction time.
   *
   * @param req_id
   * @param buf
   * @param len
   * @return true
   * @return false
   */
  bool postRecv(uint64_t req_id, void *buf, uint32_t len);

  /**
   * @brief Gets the underlying raw UD QP.
   *
   * @return struct ibv_qp*
   */
  inline struct ibv_qp *raw() const { return unique_qp.get(); }

 private:
  inline bool postSend(ibv_send_wr &wr);
  inline bool postRecv(ibv_recv_wr &wr);

  ctrl::ControlBlock &cb;
  struct ibv_pd *pd;
  struct ibv_cq *recv_cq, *send_cq;
  // We want the pointer to be moved on UD move, so we use a smart pointer.
  deleted_unique_ptr<struct ibv_qp> unique_qp;
  // Todo: is this relevant to have a single MR linked to a UD?
  ctrl::ControlBlock::MemoryRegion mr;

  LOGGER_DECL(logger);
};

template <size_t MaxBatchSz>
UdBatch<MaxBatchSz> &UdBatch<MaxBatchSz>::append(
    struct ibv_ah *ah, uint64_t req_id, void *buf, uint32_t len,
    uint32_t immediate, ctrl::ControlBlock::MemoryRegion const &mr,
    uint32_t qpn, uint32_t qkey) {
  if (i == requests.size()) {
    throw std::runtime_error("UdBatch has no more space to append");
  }

  auto &wr = requests[i].wr;
  auto &sg = requests[i].sg;

  sg = {};
  wr = {};

  sg.addr = reinterpret_cast<uint64_t>(buf);
  sg.length = len;
  sg.lkey = mr.lkey;

  wr.wr_id = req_id;
  wr.sg_list = &sg;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_SEND_WITH_IMM;
  wr.imm_data = immediate;
  wr.send_flags = IBV_SEND_SIGNALED |
                  (len > UnreliableDatagram::MaxInlining ? 0 : IBV_SEND_INLINE);
  wr.wr.ud.ah = ah;
  wr.wr.ud.remote_qpn = qpn;
  wr.wr.ud.remote_qkey = qkey;

  if (i > 0) {
    requests[i - 1].wr.next = &wr;
    requests[i - 1].wr.send_flags &= (~IBV_SEND_SIGNALED);
  }

  i++;

  return *this;
}

template <size_t MaxBatchSz>
struct ibv_send_wr &UdBatch<MaxBatchSz>::head() {
  if (i == 0) {
    throw std::runtime_error("UdBatch is empty");
  }

  return requests[0].wr;
}

/**
 * @brief Connection to a remote UD QP associated with a shared local UD QP.
 *
 */
class UnreliableDatagramConnection {
 public:
  UnreliableDatagramConnection(ctrl::ControlBlock &cb,
                               std::string const &pd_name,
                               std::shared_ptr<UnreliableDatagram> shared_ud,
                               UnreliableDatagramInfo const &info)
      : ud_info{info}, shared_ud{std::move(shared_ud)} {
    createAh(cb, pd_name);
  }

  /**
   * @brief Construct a new Unreliable Datagram Connection object using
   *        serialized information of the remote UD QP.
   *
   * @param cb
   * @param pd_name
   * @param shared_ud
   * @param serialized
   */
  UnreliableDatagramConnection(ctrl::ControlBlock &cb,
                               std::string const &pd_name,
                               std::shared_ptr<UnreliableDatagram> shared_ud,
                               std::string const &serialized)
      : shared_ud{std::move(shared_ud)} {
    deserialize(serialized);
    createAh(cb, pd_name);
  }

  // A UDC can be moved, but not copied.
  UnreliableDatagramConnection(UnreliableDatagramConnection &&) = default;
  UnreliableDatagramConnection &operator=(
      UnreliableDatagramConnection const &) = delete;
  UnreliableDatagramConnection(UnreliableDatagramConnection const &) = delete;
  // Deleted as we don't use it.
  UnreliableDatagramConnection &operator=(UnreliableDatagramConnection &&) =
      delete;

  bool postSend(uint64_t req_id, void *buf, uint32_t len);
  bool postSend(uint64_t req_id, void *buf, uint32_t len, uint32_t immediate);

  template <size_t MaxBatchSz>
  void append(UdBatch<MaxBatchSz> &batch, uint64_t req_id, void *buf,
              uint32_t len, uint32_t immediate) {
    shared_ud->append(batch, unique_ah.get(), req_id, buf, len, immediate,
                      ud_info.qpn, ud_info.qkey);
  }

  /**
   * @brief Returns the underlying shared UD QP wrapper.
   *
   * @return std::shared_ptr<UnreliableDatagram>
   */
  std::shared_ptr<UnreliableDatagram> ud() const { return shared_ud; }

 private:
  void deserialize(std::string const &serialized) {
    ud_info = UnreliableDatagramInfo::fromSerialized(serialized);
  }

  /**
   * @brief Creates and associates an Address Handler to the wrapper so that one
   *        can send requests to it.
   *
   * @param cb
   * @param pd_name
   */
  void createAh(ctrl::ControlBlock &cb, std::string const &pd_name);

  UnreliableDatagramInfo ud_info;

  std::shared_ptr<UnreliableDatagram> shared_ud;
  // We want the pointer to be moved on UDC move, so we use a smart pointer.
  deleted_unique_ptr<struct ibv_ah> unique_ah;
};

/**
 * @brief Multicast group associated with a shared local UD QP.
 *
 */
class McGroup {
 public:
  /**
   * @brief Constructs a new McGroup object using serialized information
   *        of the IBV McGroup.
   *
   * The group is automatically attached to the underlying UD QP.
   * It is detached on destruction.
   *
   * @param cb
   * @param pd_name
   * @param shared_ud
   * @param serialized
   */
  McGroup(ctrl::ControlBlock &cb, std::string const &pd_name,
          std::shared_ptr<UnreliableDatagram> const &shared_ud,
          std::string const &serialized)
      : shared_ud{shared_ud} {
    deserialize(serialized);
    createAh(cb, pd_name);

    if (ibv_attach_mcast(shared_ud->raw(), &mcud_info.gid, mcud_info.lid)) {
      throw std::runtime_error("Couldn't attach mc group.");
    }
  }

  ~McGroup() noexcept(false);

  // A McGroup can be moved, but not copied.
  McGroup(McGroup &&) = default;
  McGroup &operator=(McGroup const &) = delete;
  McGroup(McGroup const &) = delete;
  // Deleted as we don't use it.
  McGroup &operator=(McGroup &&) = delete;

  bool postSend(uint64_t req_id, void *buf, uint32_t len);
  bool postSend(uint64_t req_id, void *buf, uint32_t len, uint32_t immediate);
  // todo: chain multiple sends

  /**
   * @brief Returns the underlying shared UD QP wrapper.
   *
   * @return std::shared_ptr<UnreliableDatagram>
   */
  std::shared_ptr<UnreliableDatagram> ud() const { return shared_ud; }

 private:
  void deserialize(std::string const &serialized) {
    mcud_info = McGroupDatagramInfo::fromSerialized(serialized);
  }

  /**
   * @brief Creates and associates an Address Handler to the wrapper so that one
   *        can send requests to it.
   *
   * @param cb
   * @param pd_name
   */
  void createAh(ctrl::ControlBlock &cb, std::string const &pd_name);

  McGroupDatagramInfo mcud_info;

  std::shared_ptr<UnreliableDatagram> shared_ud;
  // We want the pointer to be moved on McGroup move, so we use a smart pointer.
  deleted_unique_ptr<struct ibv_ah> unique_ah;
  // We don't want to implement a custom move constructor, but we would like to
  // check whether the object has been moved or not in the destructor to detach
  // from the UD QP.
  MoveIndicator moved;
};

// When a datagram is received, the first 40 bytes will contain the GRH.
// We use this simple struct to dismiss all this data we don't care about.
// error: packed attribute is unnecessary for ‘ReceiveSlot’
template <typename T>
struct UdReceiveSlotImpl {
  using inner_type = T;
  uint8_t dismissed[UnreliableDatagram::UdGrhLength];
  T resp;
};

template <typename T>
struct UdReceiveSlotHelper {
  using type = UdReceiveSlotImpl<T>;
  static_assert(sizeof(type) == sizeof(T) + UnreliableDatagram::UdGrhLength);
};

template <typename T>
using UdReceiveSlot = typename UdReceiveSlotHelper<T>::type;

}  // namespace dory::conn
