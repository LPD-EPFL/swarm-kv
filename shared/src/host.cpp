#include <cerrno>
#include <climits>
#include <cstring>
#include <stdexcept>

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "host.hpp"

namespace dory {
std::pair<std::string, std::string> fqdn_and_ip(std::string const &hostname) {
  int ret;

  struct addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;  // Only IPv4 addresses
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_CANONNAME;

  struct addrinfo *info;
  ret = getaddrinfo(hostname.c_str(), "http", &hints, &info);

  if (ret != 0) {
    freeaddrinfo(info);

    int error = (ret == EAI_SYSTEM) ? errno : ret;

    throw std::runtime_error("Could not get the address info (" +
                             std::to_string(error) +
                             "): " + std::string(std::strerror(error)));
  }

  std::string canonname;
  std::string ipv4;
  for (struct addrinfo *p = info; p != nullptr; /*p = p->ai_next */) {
    canonname = p->ai_canonname;

    char ip_text[INET_ADDRSTRLEN];
    auto *sin = reinterpret_cast<struct sockaddr_in *>(p->ai_addr);

    auto const *s =
        inet_ntop(AF_INET, &sin->sin_addr, ip_text, INET_ADDRSTRLEN);

    if (s == nullptr) {
      throw std::runtime_error("Could not get the IPv4 address (" +
                               std::to_string(errno) +
                               "): " + std::string(std::strerror(errno)));
    }
    ipv4 = std::string(s);

    break;
  }

  freeaddrinfo(info);

  if (canonname.empty()) {
    throw std::runtime_error("Could not get canonical name of the host");
  }

  return std::make_pair(canonname, ipv4);
}

std::string ip_address(std::string const &hostname) {
  auto [_, ipv4] = fqdn_and_ip(hostname);
  return ipv4;
}

std::string fq_hostname() {
  char hostname[HOST_NAME_MAX + 1];
  hostname[HOST_NAME_MAX] = '\0';

  int ret = gethostname(hostname, HOST_NAME_MAX);
  if (ret == -1) {
    throw std::runtime_error("Could not get the hostname (" +
                             std::to_string(errno) +
                             "): " + std::string(std::strerror(errno)));
  }

  auto [canonname, _] = fqdn_and_ip(hostname);

  return canonname;
}
}  // namespace dory
