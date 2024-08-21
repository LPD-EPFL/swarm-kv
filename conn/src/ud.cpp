#include <cstring>
#include <stdexcept>

#include "ud.hpp"

static void cleanly_free_qp(struct ibv_qp *qp, struct ibv_cq *send_cq);
static void cleanly_free_ah(struct ibv_ah *ah);

// UnreliableDatagram
namespace dory::conn {
UnreliableDatagram::UnreliableDatagram(ctrl::ControlBlock &cb,
                                       std::string const &pd_name,
                                       std::string const &mr_name,
                                       std::string const &send_cq_name,
                                       std::string const &recv_cq_name)
    : cb{cb},
      pd{cb.pd(pd_name).get()},
      mr{cb.mr(mr_name)},
      LOGGER_INIT(logger, "UD") {
  struct ibv_qp_init_attr create_attr = {};
  create_attr.qp_type = IBV_QPT_UD;
  create_attr.cap.max_send_wr = WrDepth;
  create_attr.cap.max_recv_wr = WrDepth;
  create_attr.cap.max_send_sge = SgeDepth;
  create_attr.cap.max_recv_sge = SgeDepth;
  create_attr.cap.max_inline_data = MaxInlining;

  send_cq = create_attr.send_cq = cb.cq(send_cq_name).get();
  recv_cq = create_attr.recv_cq = cb.cq(recv_cq_name).get();

  unique_qp = deleted_unique_ptr<struct ibv_qp>(
      ibv_create_qp(pd, &create_attr),
      [=](struct ibv_qp *qp) { cleanly_free_qp(qp, send_cq); });

  if (unique_qp.get() == nullptr) {
    throw std::runtime_error("Could not create the queue pair");
  }

  /* Move the UD QP into the INIT state */
  struct ibv_qp_attr attr = {};

  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = cb.port();
  attr.qkey = DefaultQKey;

  int rc = ibv_modify_qp(
      unique_qp.get(), &attr,
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY);
  if (rc != 0) {
    throw std::runtime_error("Could not modify QP to INIT: " +
                             std::string(strerror(rc)));
  }

  /* Move the QP to RTR */
  struct ibv_qp_attr rtr_attr = {};
  rtr_attr.qp_state = IBV_QPS_RTR;

  rc = ibv_modify_qp(unique_qp.get(), &rtr_attr, IBV_QP_STATE);
  if (rc != 0) {
    throw std::runtime_error("Could not modify QP to RTR: " +
                             std::string(strerror(rc)));
  }

  /* Move the QP to RTS */
  rtr_attr.qp_state = IBV_QPS_RTS;
  rtr_attr.sq_psn = DefaultPsn;

  rc = ibv_modify_qp(unique_qp.get(), &rtr_attr, IBV_QP_STATE | IBV_QP_SQ_PSN);
  if (rc != 0) {
    throw std::runtime_error("Could not modify QP to RTS: " +
                             std::string(strerror(rc)));
  }
}

UnreliableDatagramInfo UnreliableDatagram::info() const {
  return UnreliableDatagramInfo(cb.lid(), unique_qp->qp_num, DefaultQKey);
}

bool UnreliableDatagram::postSend(uint64_t req_id, void *buf, uint32_t len,
                                  struct ibv_ah *ah, uint32_t qpn,
                                  uint32_t qkey) {
  struct ibv_sge sg = {};
  sg.addr = reinterpret_cast<uint64_t>(buf);
  sg.length = len;
  sg.lkey = mr.lkey;

  struct ibv_send_wr wr = {};
  wr.wr_id = req_id;
  wr.sg_list = &sg;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED | (len > MaxInlining ? 0 : IBV_SEND_INLINE);
  wr.wr.ud.ah = ah;
  wr.wr.ud.remote_qpn = qpn;
  wr.wr.ud.remote_qkey = qkey;

  return postSend(wr);
}

bool UnreliableDatagram::postSend(uint64_t req_id, void *buf, uint32_t len,
                                  uint32_t immediate, struct ibv_ah *ah,
                                  uint32_t qpn, uint32_t qkey) {
  struct ibv_sge sg = {};
  sg.addr = reinterpret_cast<uint64_t>(buf);
  sg.length = len;
  sg.lkey = mr.lkey;

  struct ibv_send_wr wr = {};
  wr.wr_id = req_id;
  wr.sg_list = &sg;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_SEND_WITH_IMM;
  wr.imm_data = immediate;
  wr.send_flags = IBV_SEND_SIGNALED | (len > MaxInlining ? 0 : IBV_SEND_INLINE);
  wr.wr.ud.ah = ah;
  wr.wr.ud.remote_qpn = qpn;
  wr.wr.ud.remote_qkey = qkey;

  return postSend(wr);
}

bool UnreliableDatagram::postRecv(uint64_t req_id, void *buf, uint32_t len) {
  struct ibv_sge sg = {};
  sg.addr = reinterpret_cast<uint64_t>(buf);
  sg.length = len + UdGrhLength;
  sg.lkey = mr.lkey;

  struct ibv_recv_wr wr = {};
  wr.wr_id = req_id;
  wr.sg_list = &sg;
  wr.num_sge = 1;
  wr.next = nullptr;

  return postRecv(wr);
}

bool UnreliableDatagram::postSend(ibv_send_wr &wr) {
  struct ibv_send_wr *bad_wr = nullptr;

  int rc = ibv_post_send(unique_qp.get(), &wr, &bad_wr);

  if (bad_wr != nullptr) {
    LOGGER_DEBUG(logger, "Got bad wr with id: {}", bad_wr->wr_id);
    return false;
  }

  if (rc != 0) {
    throw std::runtime_error(
        "Error due to driver misuse during posting a SEND: " +
        std::string(std::strerror(rc)));
  }

  return true;
}

bool UnreliableDatagram::postRecv(ibv_recv_wr &wr) {
  struct ibv_recv_wr *bad_wr = nullptr;

  int rc = ibv_post_recv(unique_qp.get(), &wr, &bad_wr);

  if (bad_wr != nullptr) {
    LOGGER_DEBUG(logger, "Got bad wr with id: {}", bad_wr->wr_id);
    return false;
  }

  if (rc != 0) {
    throw std::runtime_error(
        "Error due to driver misuse during posting a RECV: " +
        std::string(std::strerror(rc)));
  }

  return true;
}
}  // namespace dory::conn

// UnreliableDatagramConnection
namespace dory::conn {

bool UnreliableDatagramConnection::postSend(uint64_t req_id, void *buf,
                                            uint32_t len) {
  return shared_ud->postSend(req_id, buf, len, unique_ah.get(), ud_info.qpn,
                             ud_info.qkey);
}

bool UnreliableDatagramConnection::postSend(uint64_t req_id, void *buf,
                                            uint32_t len, uint32_t immediate) {
  return shared_ud->postSend(req_id, buf, len, immediate, unique_ah.get(),
                             ud_info.qpn, ud_info.qkey);
}

void UnreliableDatagramConnection::createAh(ctrl::ControlBlock &cb,
                                            std::string const &pd_name) {
  struct ibv_ah_attr ah_attr = {};
  ah_attr.is_global = 0;
  ah_attr.dlid = ud_info.lid;
  ah_attr.sl = 0;
  ah_attr.src_path_bits = 0;
  ah_attr.port_num = cb.port();

  struct ibv_ah *raw_ah = ibv_create_ah(cb.pd(pd_name).get(), &ah_attr);
  if (raw_ah == nullptr) {
    throw std::runtime_error("Could not create AH");
  }

  unique_ah = deleted_unique_ptr<struct ibv_ah>(raw_ah, cleanly_free_ah);
}
}  // namespace dory::conn

// McGroup
namespace dory::conn {

McGroup::~McGroup() noexcept(false) {
  if (moved) {
    return;
  }

  int rc = ibv_detach_mcast(shared_ud->raw(), &mcud_info.gid, mcud_info.lid);
  if (rc != 0) {
    throw std::runtime_error("Could not detach MC from UD QP: " +
                             std::string(strerror(rc)));
  }
}

bool McGroup::postSend(uint64_t req_id, void *buf, uint32_t len) {
  return shared_ud->postSend(req_id, buf, len, unique_ah.get(),
                             UnreliableDatagram::McQpn,
                             UnreliableDatagram::DefaultQKey);
}

bool McGroup::postSend(uint64_t req_id, void *buf, uint32_t len,
                       uint32_t immediate) {
  return shared_ud->postSend(req_id, buf, len, immediate, unique_ah.get(),
                             UnreliableDatagram::McQpn,
                             UnreliableDatagram::DefaultQKey);
}

void McGroup::createAh(ctrl::ControlBlock &cb, std::string const &pd_name) {
  struct ibv_ah_attr ah_attr = {};
  ah_attr.is_global = 1;
  ah_attr.dlid = mcud_info.lid;
  ah_attr.sl = 0;
  ah_attr.src_path_bits = 0;
  ah_attr.port_num = cb.port();

  memcpy(&ah_attr.grh.dgid, &mcud_info.gid, sizeof(ah_attr.grh.dgid));

  struct ibv_ah *raw_ah = ibv_create_ah(cb.pd(pd_name).get(), &ah_attr);
  if (raw_ah == nullptr) {
    throw std::runtime_error("Could not create AH");
  }

  unique_ah = deleted_unique_ptr<struct ibv_ah>(raw_ah, cleanly_free_ah);
}
}  // namespace dory::conn

/**
 * @brief Advanced QP destructor-like that also empties the associated Cq and
 *        resets the QP before destroying it.
 *
 * Note: this is maybe not a desired feature.
 *
 * @param qp
 * @param send_cq
 */
void cleanly_free_qp(struct ibv_qp *qp, struct ibv_cq *send_cq) {
  int rc;

  /* Move the QP into the ERR state to cancel all outstanding
      work requests */
  struct ibv_qp_attr attr = {};
  attr.qp_state = IBV_QPS_ERR;
  attr.sq_psn = 0;

  rc = ibv_modify_qp(qp, &attr, IBV_QP_STATE);
  if (0 != rc) {
    throw std::runtime_error("Could not modify QP to ERR: " +
                             std::string(strerror(rc)));
  }

  struct ibv_wc wc;
  while (ibv_poll_cq(send_cq, 1, &wc) > 0) {
  }

  /* move the QP into the RESET state */
  attr.qp_state = IBV_QPS_RESET;

  rc = ibv_modify_qp(qp, &attr, IBV_QP_STATE);
  if (0 != rc) {
    throw std::runtime_error("Could not modify QP to RESET: " +
                             std::string(strerror(rc)));
  }

  rc = ibv_destroy_qp(qp);
  if (0 != rc) {
    throw std::runtime_error("Could not destroy the QP: " +
                             std::string(strerror(rc)));
  }
}

/**
 * @brief Advanced AH destructor-like that checks for destruction success.
 *
 * @param ah
 */
void cleanly_free_ah(struct ibv_ah *ah) {
  int rc = ibv_destroy_ah(ah);

  if (rc != 0) {
    throw std::runtime_error("Could not destroy the AH: " +
                             std::string(strerror(rc)));
  }
}
