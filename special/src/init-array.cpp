#include "init-array.hpp"

namespace dory::special {
ProcessArguments::ProcessArguments(int argc, char **argv, char **envp)
    : argc_{argc}, argv_{argv}, envp_{envp} {
  copyArgv();
  copyEnvp();
}

void ProcessArguments::copyArgv() {
  if (argv_ == nullptr) {
    return;
  }

  while (argc_ > 0) {
    argv_copy.emplace_back(*argv_);
    argv_++;
    argc_--;
  }
}

void ProcessArguments::copyEnvp() {
  if (envp_ == nullptr) {
    return;
  }

  while (*envp_) {
    envp_copy.emplace_back(*envp_);
    envp_++;
  }
}
}  // namespace dory::special

namespace dory::special {

// Dummy struct to determine the order of initialization.
// GCC and Clang differ in the order of initializing the global objects and the
// .init_array constructors. In GCC, the global objects are constructed before
// the .init_array, while in Clang the opposite holds.
// The Dummy struct determined this behaviour during the runtime.
struct Dummy {
  Dummy(bool &touch_variable) { touch_variable = true; }
};

bool statics_initialized = false;
Dummy dummy(statics_initialized);

int argc = 0;
char **argv = nullptr;
char **envp = nullptr;
ProcessArguments const processArguments(argc, argv, envp);

static void store_arguments(int argc_, char **argv_, char **envp_) {
  // Store the arguments in global variables.
  // In Clang, the processArguments object will be constructed after the
  // .init_array, so it will have the correct content.
  argc = argc_;
  argv = argv_;
  envp = envp_;

  // In GCC, the object is already constructed, so we can copy-construct it
  // with the correct content.
  if (statics_initialized) {
    auto &pa = const_cast<ProcessArguments &>(processArguments);
    pa = ProcessArguments(argc, argv, envp);
  }
}

/* Put the function into the init_array */
__attribute__((section(".init_array"))) void *ctr =
    reinterpret_cast<void *>(&store_arguments);

}  // namespace dory::special
