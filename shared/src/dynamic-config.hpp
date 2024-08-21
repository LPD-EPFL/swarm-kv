#include <unistd.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <toml.hpp>

namespace dory::tests {

using ProcIdType = uint16_t;
using Host = std::pair<std::string, ProcIdType>;

/**
 * @brief Struct holding the data parsed from a dynamic configuration generated
 *        by deploy.py. It is shared so that it can be reused accross tests.
 *
 */
struct DynamicConfig {
  std::map<std::string, ProcIdType> hosts;
  std::vector<ProcIdType> ids;
  std::vector<ProcIdType> remote_ids;
  ProcIdType my_id;
  size_t index_on_machine = 0;

  DynamicConfig(std::string const& config_path) {
    // Parsing dynamic configuration
    auto dynamic_config = toml::parse_file(config_path);
    if (!dynamic_config["hosts"].is_table()) {
      throw std::runtime_error("'hosts' table doesn't exit.");
    }
    auto hosts_table = *dynamic_config["hosts"].as_table();
    if (!hosts_table.is_homogeneous(toml::node_type::integer)) {
      throw std::runtime_error("'hosts' table values should be integers.");
    }

    // Default node name is the hostname.
    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, HOST_NAME_MAX) < 0) {
      throw std::runtime_error("Failed to retrieve hostname.");
    }
    // We tolerate node name overwriting
    char* custom_node_name = getenv("DORY_NODE_NAME");
    char* node_name = custom_node_name ? custom_node_name : hostname;

    // If there are multiple nodes on the same machine, we get our index
    if (custom_node_name) {
      extractIndex(custom_node_name);
    }

    if (!hosts_table[node_name]) {
      throw std::runtime_error("Could not find id for " +
                               std::string(node_name) + ".");
    }
    my_id = *hosts_table[node_name].value<ProcIdType>();

    for (auto&& [host, id_node] : hosts_table) {
      auto id = *id_node.value<ProcIdType>();
      hosts.insert({host, id});
      ids.push_back(*id_node.value<ProcIdType>());
      if (id != my_id) {
        remote_ids.push_back(id);
      }
    }

    if (std::any_of(ids.cbegin(), ids.cend(),
                    [](auto id) { return id == 0; })) {
      throw std::runtime_error("Ids should be > 0.");
    }
  }

  /**
   * @brief Extracts the index of this node on the machine from its node name.
   *
   * @param name should have at most one '-' and this '-' should be followed by
   *        an integer.
   */
  void extractIndex(char const* name) {
    auto node_name = std::string(name);
    std::replace(node_name.begin(), node_name.end(), '-', ' ');
    std::istringstream iss(node_name);
    std::vector<std::string> vec(std::istream_iterator<std::string>{iss},
                                 std::istream_iterator<std::string>());
    if (vec.size() > 2) {
      throw std::runtime_error("Invalid node name");
    }
    if (vec.size() == 1) {
      return;
    }
    index_on_machine = static_cast<size_t>(std::stoi(vec[1]));
  }
};

}  // namespace dory::tests
