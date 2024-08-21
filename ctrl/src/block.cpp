#include <cstring>
#include <stdexcept>

#include "block.hpp"
#include "device.hpp"

namespace dory::ctrl {
ControlBlock::ControlBlock(ResolvedPort &resolved_port)
    : resolved_port{resolved_port}, LOGGER_INIT(logger, "CB") {}

void ControlBlock::registerPd(std::string const &name) {
  if (pds.find(name) != pds.end()) {
    throw std::runtime_error("Already registered protection domain named " +
                             name);
  }

  auto *pd = ibv_alloc_pd(resolved_port.device().context());

  if (pd == nullptr) {
    throw std::runtime_error("Could not register the protection domain " +
                             name);
  }

  deleted_unique_ptr<struct ibv_pd> uniq_pd(pd, [](struct ibv_pd *pd) {
    auto ret = ibv_dealloc_pd(pd);
    if (ret != 0) {
      throw std::runtime_error("Could not dealloc pd: " +
                               std::string(std::strerror(errno)));
    }
  });

  pds.emplace(name, std::move(uniq_pd));
  LOGGER_INFO(logger, "PD '{}' registered", name);
}

deleted_unique_ptr<struct ibv_pd> &ControlBlock::pd(std::string const &name) {
  auto pd = pds.find(name);
  if (pd == pds.end()) {
    throw std::runtime_error("Protection domain named " + name +
                             " does not exist");
  }

  return pd->second;
}

void ControlBlock::allocateBuffer(std::string const &name, size_t length,
                                  size_t alignment) {
  if (buf_map.find(name) != buf_map.end()) {
    throw std::runtime_error("Already registered buffer named " + name);
  }

  std::shared_ptr<uint8_t> data(allocate_aligned<uint8_t>(alignment, length),
                                DeleteAligned<uint8_t>());
  memset(data.get(), 0, length);

  raw_bufs.push_back(std::move(data));

  std::pair<size_t, size_t> index_length(raw_bufs.size() - 1, length);

  buf_map.insert({name, index_length});
  LOGGER_INFO(logger, "Buffer '{}' of size {} allocated", name, length);
}

#ifdef DORY_CTRL_DM
void ControlBlock::allocateDm(std::string const &name, size_t length,
                              size_t alignment) {
  if (dms.find(name) != dms.end()) {
    throw std::runtime_error("Already registered DM named " + name);
  }

  struct ibv_alloc_dm_attr alloc_dm_attr = {};
  alloc_dm_attr.length = length;
  alloc_dm_attr.log_align_req =
      static_cast<uint32_t>(std::ceil(std::log2(alignment)));

  struct ibv_dm *const dm =
      ibv_alloc_dm(resolved_port.device().context(), &alloc_dm_attr);
  if (dm == nullptr) {
    throw std::runtime_error("Failed to allocate DM named " + name);
  }

  std::shared_ptr<struct ibv_dm> shared_dm(dm, ibv_free_dm);
  std::vector<uint8_t> zeroed_buffer(length);
  int const cpy_ret = ibv_memcpy_to_dm(dm, 0, &zeroed_buffer[0], length);
  if (cpy_ret != 0) {
    throw std::runtime_error("Failed to zero DM " + name);
  }

  dms.emplace(name, DeviceMemory(shared_dm, length));
  LOGGER_INFO(logger, "DM '{}' of size {} allocated", name, length);
}

ControlBlock::DeviceMemory ControlBlock::dm(std::string const &name) const {
  auto dm = dms.find(name);
  if (dm == dms.end()) {
    throw std::runtime_error("DM named " + name + " does not exist");
  }

  return dm->second;
}
#endif

void ControlBlock::allocatePhysicallyLockedBuffer(
    std::string const &name, size_t length,
    memory::PhysicallyLockedBuffer::AllocationPool allocation_pool) {
  if (buf_map.find(name) != buf_map.end()) {
    throw std::runtime_error("Already registered protection domain named " +
                             name);
  }

  auto *locked_buf =
      new memory::PhysicallyLockedBuffer(length, allocation_pool);
  auto *underlying_buf = static_cast<uint8_t *>(locked_buf->ptr());

  std::shared_ptr<uint8_t> data(
      underlying_buf,
      DeleteRedirected<uint8_t, memory::PhysicallyLockedBuffer>(locked_buf));
  memset(data.get(), 0, length);

  raw_bufs.push_back(std::move(data));

  std::pair<size_t, size_t> index_length(raw_bufs.size() - 1, length);

  buf_map.insert({name, index_length});
  LOGGER_INFO(logger, "Buffer '{}' of size {} allocated", name, length);
}

void ControlBlock::registerMr(std::string const &name,
                              std::string const &pd_name,
                              std::string const &buffer_name, size_t offset,
                              size_t buf_len, MemoryRights rights) {
  if (mrs.find(name) != mrs.end()) {
    throw std::runtime_error("Already registered memory region named " + name);
  }
  auto pd = pds.find(pd_name);
  if (pd == pds.end()) {
    throw std::runtime_error("No PD exists with name " + pd_name);
  }

  auto buf = buf_map.find(buffer_name);
  if (buf == buf_map.end()) {
    throw std::runtime_error("No buffer exists with name " + buffer_name);
  }

  auto *mr =
      ibv_reg_mr(pd->second.get(), &(raw_bufs[buf->second.first].get()[offset]),
                 buf_len, static_cast<int>(rights));

  if (mr == nullptr) {
    throw std::runtime_error("Could not register the memory region " + name);
  }

  deleted_unique_ptr<struct ibv_mr> uniq_mr(mr, [](struct ibv_mr *mr) {
    auto ret = ibv_dereg_mr(mr);
    if (ret != 0) {
      throw std::runtime_error("Could not dereg mr: " +
                               std::string(std::strerror(errno)));
    }
  });

  mrs.emplace(name, std::move(uniq_mr));
  LOGGER_INFO(logger,
              "Mr '{}' under PD '{}' registered with buf '{}' (offset: "
              "{}, length: {}) and rights {}",
              name, pd_name, buffer_name, offset, buf_len, rights);
}

void ControlBlock::registerMr(std::string const &name,
                              std::string const &pd_name,
                              std::string const &buffer_name,
                              MemoryRights rights) {
  auto buf = buf_map.find(buffer_name);
  if (buf == buf_map.end()) {
    throw std::runtime_error("No buffer exists with name " + buffer_name);
  }
  registerMr(name, pd_name, buffer_name, 0, buf->second.second, rights);
}

#ifdef DORY_CTRL_DM
void ControlBlock::registerDmMr(std::string const &name,
                                std::string const &pd_name,
                                std::string const &dm_name, size_t offset,
                                size_t buf_len, MemoryRights rights) {
  if (mrs.find(name) != mrs.end()) {
    throw std::runtime_error("Already registered memory region named " + name);
  }
  auto pd = pds.find(pd_name);
  if (pd == pds.end()) {
    throw std::runtime_error("No PD exists with name " + pd_name);
  }

  auto dm = dms.find(dm_name);
  if (dm == dms.end()) {
    throw std::runtime_error("No DM exists with name " + dm_name);
  }

  auto *mr =
      ibv_reg_dm_mr(pd->second.get(), dm->second.dm.get(), offset, buf_len,
                    static_cast<unsigned int>(rights) | IBV_ACCESS_ZERO_BASED);

  if (mr == nullptr) {
    throw std::runtime_error("Could not DM-register the memory region " + name);
  }

  deleted_unique_ptr<struct ibv_mr> uniq_mr(mr, [](struct ibv_mr *mr) {
    auto ret = ibv_dereg_mr(mr);
    if (ret != 0) {
      throw std::runtime_error("Could not dereg mr: " +
                               std::string(std::strerror(errno)));
    }
  });

  mrs.emplace(name, std::move(uniq_mr));
  LOGGER_INFO(logger,
              "DM Mr '{}' under PD '{}' registered with buf '{}' (offset: "
              "{}, length: {}) and rights {}",
              name, pd_name, dm_name, offset, buf_len, rights);
}

void ControlBlock::registerDmMr(std::string const &name,
                                std::string const &pd_name,
                                std::string const &dm_name,
                                MemoryRights rights) {
  auto dm = dms.find(dm_name);
  if (dm == dms.end()) {
    throw std::runtime_error("No DM buffer exists with name " + dm_name);
  }

  registerDmMr(name, pd_name, dm_name, 0, dm->second.size, rights);
}
#endif

ControlBlock::MemoryRegion ControlBlock::mr(std::string const &name) const {
  auto mr = mrs.find(name);
  if (mr == mrs.end()) {
    throw std::runtime_error("Memory region named " + name + " does not exist");
  }

  auto const &region = mr->second;

  MemoryRegion m;
  m.addr = reinterpret_cast<uintptr_t>(region->addr);
  m.size = region->length;
  m.lkey = region->lkey;
  m.rkey = region->rkey;

  return m;
}

// void ControlBlock::withdrawMrRight(std::string const &name) const {
//   auto mr = mr_map.find(name);
//   if (mr == mr_map.end()) {
//     throw std::runtime_error("Memory region named " + name + " does not
//     exist");
//   }

//   auto const& region = mrs[mr->second];

//   auto ret = ibv_rereg_mr(region.get(), IBV_REREG_Mr_CHANGE_ACCESS, NULL,
//   NULL,
//                           region->length, IBV_ACCESS_LOCAL_WRITE);

//   if (ret != 0) {
//     throw std::runtime_error("Memory region named " + name + " cannot be
//     withdrawn");
//   }
// }

void ControlBlock::registerCq(std::string const &name) {
  if (cqs.find(name) != cqs.end()) {
    throw std::runtime_error("Already registered completion queue named " +
                             name);
  }

  auto *cq = ibv_create_cq(resolved_port.device().context(), CqDepth, nullptr,
                           nullptr, 0);

  if (cq == nullptr) {
    throw std::runtime_error("Could not register the completion queue " + name);
  }

  deleted_unique_ptr<struct ibv_cq> uniq_cq(cq, [name](struct ibv_cq *cq) {
    auto ret = ibv_destroy_cq(cq);
    if (ret != 0) {
      throw std::runtime_error("Could not destroy Cq " + name + ": " +
                               std::string(std::strerror(errno)));
    }
  });

  cqs.emplace(name, std::move(uniq_cq));
  LOGGER_INFO(logger, "Cq '{}' registered", name);
}

deleted_unique_ptr<struct ibv_cq> &ControlBlock::cq(std::string const &name) {
  auto cq = cqs.find(name);
  if (cq == cqs.end()) {
    throw std::runtime_error("Completion queue named " + name +
                             " does not exist");
  }

  return cq->second;
}

uint8_t ControlBlock::port() const { return resolved_port.portId(); }

uint16_t ControlBlock::lid() const { return resolved_port.portLid(); }

bool ControlBlock::pollCqIsOk(deleted_unique_ptr<struct ibv_cq> &cq,
                              std::vector<struct ibv_wc> &entries) {
  auto num =
      ibv_poll_cq(cq.get(), static_cast<int>(entries.size()), &entries[0]);

  if (num >= 0) {
    entries.erase(entries.begin() + num, entries.end());
    return true;
  }
  return false;
}
}  // namespace dory::ctrl
