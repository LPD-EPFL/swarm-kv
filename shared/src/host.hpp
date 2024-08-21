#pragma once

#include <string>
#include <utility>

namespace dory {
std::pair<std::string, std::string> fqdn_and_ip(std::string const &hostname);

std::string ip_address(std::string const &hostname);

std::string fq_hostname();
}  // namespace dory
