#include "heartbeat.hpp"

// Non-ISO headers for POSIX and Linux specifics
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <stdexcept>

#include <dory/special/proc-name.hpp>

#ifdef ENABLE_RETAINER
namespace dory::memory::internal {
static void heartbeat_child_trap_handler(int sig);
static void heartbeat_child_int_handler(int sig);

StartUpHeartBeatFork::StartUpHeartBeatFork() {
  setupPipe();
  forkMemoryRetainer();
}

void StartUpHeartBeatFork::startMemoryRetainer(std::string const &memoryName) {
  size_t nwritten = 0;
  ssize_t ret = 0;
  while (nwritten < memoryName.size() + 1) {
    ret = write(pipefd[1], memoryName.c_str() + nwritten,
                memoryName.size() + 1 - nwritten);
    if (ret <= 0) {
      break;
    }

    nwritten += static_cast<size_t>(ret);
  }

  if (ret <= 0) {
    throw std::runtime_error(
        "The heartbeat parent process could not share the memfd "
        "with the child (" +
        std::to_string(errno) + "): " + std::string(std::strerror(errno)));
  }

  close(pipefd[1]);

  // Block until the child signals success
  char read_signal;
  ssize_t read_signal_ret =
      read(signalfd[0], &read_signal, sizeof(read_signal));
  if (read_signal_ret <= 0) {
    throw std::runtime_error(
        "The heartbeat child process had some issue with "
        "mapping the file-backed memory region (" +
        std::to_string(errno) + "): " + std::string(std::strerror(errno)));
  }
}

void StartUpHeartBeatFork::killRetainer() const {
  if (pid > 0) {
    auto ret = kill(pid, SIGTERM);
    if (ret == -1) {
      throw std::runtime_error("Could not kill the heartbeat child process (" +
                               std::to_string(errno) +
                               "): " + std::string(std::strerror(errno)));
    }

    int status;
    auto wait_ret = waitpid(pid, &status, 0);
    if (wait_ret > 0) {
      if (wait_ret != pid) {
        throw std::runtime_error(
            "Forking other processes apart from the "
            "heartbeat is not supported (" +
            std::to_string(errno) + "): " + std::string(std::strerror(errno)));
      }
    } else if (wait_ret < 0) {
      throw std::runtime_error(
          "The heartbeat parent process cannot wait for the child to die (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }
  }
}

void StartUpHeartBeatFork::childTrapEntry(int sig) { childTrap(sig); }

void StartUpHeartBeatFork::childIntEntry(int /*unused*/) {
  // When using interactive shell, Ctrl-C is sent to all child processes
  // We don't want this behaviour, so we override it by doint nothing.
}

void StartUpHeartBeatFork::forkMemoryRetainer() {
  auto ppid_before_fork = getpid();
  pid = fork();

  if (pid == -1) {
    throw std::runtime_error("Could not fork heartbeat memory retainer (" +
                             std::to_string(errno) +
                             "): " + std::string(std::strerror(errno)));
  }

  if (pid == 0) {  // Child
    signal(SIGTERM, heartbeat_child_trap_handler);
    signal(SIGINT, heartbeat_child_int_handler);

    close(pipefd[1]);    // Close unused write end
    close(signalfd[0]);  // Close unused read end

    special::set_process_name("-frk");

    auto ret = prctl(PR_SET_PDEATHSIG, SIGTERM);
    if (ret == -1) {
      throw std::runtime_error(
          "Could not make heartbeat parent deliver "
          "SIGTERM to child upon its death (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }

    if (getppid() != ppid_before_fork) {
      throw std::runtime_error(
          "Heartbeat parent died before registering the SIGTERM signal (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }

    std::string mem_name;
    ssize_t r;
    char buf[2];
    buf[1] = 0;
    while ((r = read(pipefd[0], &buf, 1)) > 0) {
      mem_name.append(buf);
    }

    if (r == -1) {
      throw std::runtime_error(
          "Reading from the heartbeat pipe broke half-way (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }
    if (r == 0 && buf[0] != '\0') {
      // throw std::runtime_error(
      //     "Reading from the heartbeat pipe did not "
      //     "deliver the complete message (" +
      //     std::to_string(errno) + "): " + std::string(std::strerror(errno)));
      while (true) {
        sleep(3600);
      }
    }

    if (mem_name.empty()) {
      // Wait indefinately until the parent dies
      while (true) {
        sleep(3600);
      }
    }

    // Attach the memory
    auto memfd = open(mem_name.c_str(), O_RDWR);
    if (r == -1) {
      throw std::runtime_error(
          "Could not open the memfd transmitted through the pipe (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }

    // It is not necessary to know the exact mmap size. A single byte on the
    // mmap is enough to keep the memory reference alive
    size_t mmap_len = 1;

    auto *addr =
        mmap(nullptr, mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, memfd, 0);
    if (addr == MAP_FAILED) {
      throw std::runtime_error(
          "Could not map the file-backed memfd "
          "transmitted through the pipe (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }
    close(pipefd[0]);

    // Signal that we have successfully attached the memory
    ssize_t signal_ok_ret = write(signalfd[1], &signalOk, sizeof(char));

    if (signal_ok_ret <= 0) {
      throw std::runtime_error(
          "Could not signal the parent process that the "
          "memory was successfully mapped (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }

    close(signalfd[1]);

    // Wait indefinately until the parent dies
    while (true) {
      sleep(3600);
    }

  } else {               // Parent
    close(pipefd[0]);    // Close unused read end
    close(signalfd[1]);  // Close unused write end
  }
}

void StartUpHeartBeatFork::setupPipe() {
  int *fds[] = {pipefd, signalfd};
  for (int i = 0; i < 2; i++) {
    auto ret = pipe(fds[i]);
    if (ret == -1) {
      throw std::runtime_error(
          "Could not setup heartbeat pipe#" + std::to_string(i) + " (" +
          std::to_string(errno) + "): " + std::string(std::strerror(errno)));
    }
  }
}

void StartUpHeartBeatFork::childTrap(int /*unused*/) {
  // TODO(saggy00): Check if the sleep is necessary.
  // Currently, the child sleeps to make sure the RT core is halted before
  // deallocating the memory.
  sleep(SleepTime);
  exit(EXIT_SUCCESS);
}

StartUpHeartBeatFork startUpHeartBeatForkInternal;
StartUpHeartBeatFork *startUpHeartBeatFork = &startUpHeartBeatForkInternal;

static void heartbeat_child_trap_handler(int sig) {
  startUpHeartBeatFork->childTrapEntry(sig);
}

static void heartbeat_child_int_handler(int sig) {
  startUpHeartBeatFork->childIntEntry(sig);
}
}  // namespace dory::memory::internal
#else
namespace dory::memory::internal {
StartUpHeartBeatFork startUpHeartBeatForkInternal;
StartUpHeartBeatFork *startUpHeartBeatFork = &startUpHeartBeatForkInternal;
}  // namespace dory::memory::internal
#endif
