#pragma once

#include <map>
#include <string>
#include <vector>

#include <dory/extern/ibverbs.hpp>

namespace dory::ctrl {
class OpenDevice {
 public:
  OpenDevice();
  OpenDevice(struct ibv_device *device);

  ~OpenDevice();

  // Copy constructor
  OpenDevice(OpenDevice const &o);

  // Move constructor
  OpenDevice(OpenDevice &&o) noexcept;

  // Copy assignment operator
  OpenDevice &operator=(OpenDevice const &o);

  // Move assignment operator
  OpenDevice &operator=(OpenDevice &&o) noexcept;

  struct ibv_context *context() {
    return ctx;
  }

  char *name() const { return dev->name; }

  char *devName() const { return dev->dev_name; }

  uint64_t guid() const { return ibv_get_device_guid(dev); }

  enum NodeType : int8_t { UNKNOWN_NODE = -1, CA = 1, RNIC = 4 };

  static char const *typeStr(NodeType t) {
    std::map<NodeType, char const *const> my_enum_strings{
        {NodeType::UNKNOWN_NODE, "NodeType::UNKNOWN"},
        {NodeType::CA, "NodeType::CA"},
        {NodeType::RNIC, "NodeType::RNIC"}};
    auto it = my_enum_strings.find(t);
    return it == my_enum_strings.end() ? "Out of range" : it->second;
  }

  NodeType nodeType() const { return static_cast<NodeType>(dev->node_type); }

  enum TransportType : int8_t { UNKNOWN_TRANSPORT = -1, IB = 0, IWARP = 1 };

  static char const *typeStr(TransportType t) {
    std::map<TransportType, char const *const> my_enum_strings{
        {TransportType::UNKNOWN_TRANSPORT, "TransportType::UNKNOWN"},
        {TransportType::IB, "TransportType::IB"},
        {TransportType::IWARP, "TransportType::IWARP"}};
    auto it = my_enum_strings.find(t);
    return it == my_enum_strings.end() ? "Out of range" : it->second;
  }

  TransportType transportType() const {
    return static_cast<TransportType>(dev->transport_type);
  }

  struct ibv_device_attr const &deviceAttributes() const;

  struct ibv_device_attr_ex const &extendedAttributes() const;

  // printf("IB device %d:\n", dev_i);
  // printf("    Name: %s\n", dev_list[dev_i]->name);
  // printf("    Device name: %s\n", dev_list[dev_i]->dev_name);
  // printf("    GUID: %zx\n",
  //        static_cast<size_t>(ibv_get_device_guid(dev_list[dev_i])));
  // printf("    Node type: %d (-1: UNKNOWN, 1: CA, 4: RNIC)\n",
  //        dev_list[dev_i]->node_type);
  // printf("    Transport type: %d (-1: UNKNOWN, 0: IB, 1: IWARP)\n",
  //        dev_list[dev_i]->transport_type);

  // printf("    fw: %s\n", device_attr.fw_ver);
  // printf("    max_qp: %d\n", device_attr.max_qp);
  // printf("    max_cq: %d\n", device_attr.max_cq);
  // printf("    max_mr: %d\n", device_attr.max_mr);
  // printf("    max_pd: %d\n", device_attr.max_pd);
  // printf("    max_ah: %d\n", device_attr.max_ah);
  // printf("    phys_port_cnt: %u\n", device_attr.phys_port_cnt);

 private:
  struct ibv_device *dev = nullptr;
  struct ibv_context *ctx = nullptr;
  struct ibv_device_attr_ex device_attr_ex = {};
};
}  // namespace dory::ctrl

namespace dory::ctrl {
class Devices {
 public:
  Devices();
  Devices(Devices const &) = delete;
  Devices &operator=(Devices const &) = delete;
  Devices(Devices && /*o*/) noexcept;
  Devices &operator=(Devices && /*o*/) noexcept;
  ~Devices();

  std::vector<OpenDevice> &list(bool force = false);

 private:
  struct ibv_device **dev_list{nullptr};
  std::vector<OpenDevice> devices;
};
}  // namespace dory::ctrl

namespace dory::ctrl {
class ResolvedPort {
 public:
  ResolvedPort(OpenDevice &od);

  /**
   * @param index: 0-based
   **/
  bool bindTo(size_t index);

  /**
   * @returns 1-based port id
   **/
  uint8_t portId() const { return port_id; }

  uint16_t portLid() const { return port_lid; }

  OpenDevice &device() { return open_dev; }

 private:
  static std::string linkLayerStr(uint8_t link_layer) {
    switch (link_layer) {
      case IBV_LINK_LAYER_UNSPECIFIED:
        return "[Unspecified]";
      case IBV_LINK_LAYER_INFINIBAND:
        return "[InfiniBand]";
      case IBV_LINK_LAYER_ETHERNET:
        return "[Ethernet]";
      default:
        return "[Invalid]";
    }
  }

  OpenDevice &open_dev;
  int port_index;
  uint8_t port_id;
  uint16_t port_lid;
};
}  // namespace dory::ctrl
