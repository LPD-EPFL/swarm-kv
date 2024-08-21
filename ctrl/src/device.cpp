#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

#include "device.hpp"

// OpenDevice definitions
namespace dory::ctrl {
OpenDevice::OpenDevice() = default;

OpenDevice::OpenDevice(struct ibv_device *device) : dev{device} {
  ctx = ibv_open_device(device);
  if (ctx == nullptr) {
    throw std::runtime_error("Could not get device list: " +
                             std::string(std::strerror(errno)));
  }

  if (ibv_query_device_ex(ctx, nullptr, &device_attr_ex) != 0) {
    throw std::runtime_error("Could not query device: " +
                             std::string(std::strerror(errno)));
  }
}

OpenDevice::~OpenDevice() {
  if (ctx != nullptr) {
    ibv_close_device(ctx);
  }
}

// Copy constructor
OpenDevice::OpenDevice(OpenDevice const &o) : dev{o.dev} {
  ctx = ibv_open_device(dev);
  if (ctx == nullptr) {
    throw std::runtime_error("Could not get device list: " +
                             std::string(std::strerror(errno)));
  }

  if (ibv_query_device_ex(ctx, nullptr, &device_attr_ex) != 0) {
    throw std::runtime_error("Could not query device: " +
                             std::string(std::strerror(errno)));
  }
}

// Move constructor
OpenDevice::OpenDevice(OpenDevice &&o) noexcept
    : dev{o.dev}, ctx{o.ctx}, device_attr_ex(o.device_attr_ex) {
  o.ctx = nullptr;
}

// Copy assignment operator
OpenDevice &OpenDevice::operator=(OpenDevice const &o) {
  if (&o == this) {
    return *this;
  }

  ctx = ibv_open_device(o.dev);
  if (ctx == nullptr) {
    throw std::runtime_error("Could not get device list: " +
                             std::string(std::strerror(errno)));
  }

  if (ibv_query_device_ex(ctx, nullptr, &device_attr_ex) != 0) {
    throw std::runtime_error("Could not query device: " +
                             std::string(std::strerror(errno)));
  }

  return *this;
}

// Move assignment operator
OpenDevice &OpenDevice::operator=(OpenDevice &&o) noexcept {
  if (&o == this) {
    return *this;
  }

  dev = o.dev;
  ctx = o.ctx;
  device_attr_ex = o.device_attr_ex;
  o.ctx = nullptr;

  return *this;
}

struct ibv_device_attr const &OpenDevice::deviceAttributes() const {
  return device_attr_ex.orig_attr;
}

struct ibv_device_attr_ex const &OpenDevice::extendedAttributes() const {
  return device_attr_ex;
}
}  // namespace dory::ctrl

// Device definitions
namespace dory::ctrl {
Devices::Devices() = default;

Devices::Devices(Devices &&o) noexcept : dev_list{o.dev_list} {
  o.dev_list = nullptr;
}

Devices &Devices::operator=(Devices &&o) noexcept {
  dev_list = o.dev_list;
  o.dev_list = nullptr;
  return *this;
}

Devices::~Devices() {
  if (dev_list != nullptr) {
    ibv_free_device_list(dev_list);
  }
}

std::vector<OpenDevice> &Devices::list(bool force) {
  if (force || dev_list == nullptr) {
    int num_devices = 0;
    dev_list = ibv_get_device_list(&num_devices);

    if (dev_list == nullptr) {
      throw std::runtime_error("Error getting device list: " +
                               std::string(std::strerror(errno)));
    }

    for (int i = 0; i < num_devices; i++) {
      devices.emplace_back(dev_list[i]);
    }

    if (devices.empty()) {
      throw std::runtime_error("No IB devices were found.");
    }
  }

  return devices;
}
}  // namespace dory::ctrl

namespace dory::ctrl {
ResolvedPort::ResolvedPort(OpenDevice &od)
    : open_dev{od}, port_index{-1}, port_id{0}, port_lid{0} {
  (void)port_index;
}

bool ResolvedPort::bindTo(size_t index) {
  size_t skipped_active_ports = 0;
  for (uint8_t i = 1; i <= open_dev.deviceAttributes().phys_port_cnt; i++) {
    struct ibv_port_attr port_attr = {};

    if (ibv_query_port(open_dev.context(), i, &port_attr)) {
      throw std::runtime_error("Failed to query port: " +
                               std::string(std::strerror(errno)));
    }

    if (port_attr.phys_state != IBV_PORT_ACTIVE &&
        port_attr.phys_state != IBV_PORT_ACTIVE_DEFER) {
      continue;
    }

    if (skipped_active_ports == index) {
      if (port_attr.link_layer != IBV_LINK_LAYER_INFINIBAND) {
        throw std::runtime_error(
            "Transport type required is InfiniBand but port link layer is " +
            linkLayerStr(port_attr.link_layer));
      }

      port_id = i;
      port_lid = port_attr.lid;

      return true;
    }

    skipped_active_ports += 1;
  }

  return false;
}
}  // namespace dory::ctrl
