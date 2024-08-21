#pragma once

#include <iostream>
#include <string>
#include <vector>

namespace dory::special {
class ProcessArguments {
 public:
  ProcessArguments() = default;
  ProcessArguments(int argc, char **argv, char **envp);

  void printArgv() const {
    for (const auto &str : argv_copy) {
      std::cout << str << std::endl;
    }
  }

  void printEnvp() const {
    for (const auto &str : envp_copy) {
      std::cout << str << std::endl;
    }
  }

  int argc() const { return argc_; }
  char const **argv() const { return const_cast<char const **>(argv_); }

 private:
  void copyArgv();
  void copyEnvp();

  int argc_;
  char **argv_;
  char **envp_;

  std::vector<std::string> argv_copy;
  std::vector<std::string> envp_copy;
};
}  // namespace dory::special

namespace dory::special {
extern ProcessArguments const processArguments;
}
