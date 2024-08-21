#include <thread>
#include <chrono>
#include "server.hpp"

int main() {
  dory::race::Server server(1, {2}, 10);
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return 0;
}