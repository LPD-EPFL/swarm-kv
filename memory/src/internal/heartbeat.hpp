#pragma once

#include <stdexcept>
#include <string>

#include <sys/types.h>

namespace dory::memory::internal {
#ifdef ENABLE_RETAINER
class StartUpHeartBeatFork {
 public:
  StartUpHeartBeatFork();
  void startMemoryRetainer(std::string const &memoryName);
  void killRetainer() const;

  // Called by the signal handler of the child
  static void childTrapEntry(int sig);
  void childIntEntry(int sig);

 private:
  void forkMemoryRetainer();
  void setupPipe();

  static void childTrap(int sig);

  int pipefd[2];
  int signalfd[2];
  pid_t pid = 0;
  char const signalOk = '!';
  static int constexpr SleepTime = 1;
};
#else
class StartUpHeartBeatFork {
 public:
  StartUpHeartBeatFork() = default;

  // Suppress linting to avoid converting these methods to static
  void startMemoryRetainer(std::string const & /*memoryName*/) {  // NOLINT
    throw std::runtime_error("Please enable retainer");
  }

  void killRetainer() {  // NOLINT
    throw std::runtime_error("Please enable retainer");
  }
};
#endif

extern StartUpHeartBeatFork *startUpHeartBeatFork;
}  // namespace dory::memory::internal
