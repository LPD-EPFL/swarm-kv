#include <dory/third-party/setproctitle/title.hpp>
#include <utility>

#include "init-array.hpp"
#include "proc-name.hpp"

namespace dory::special {
void set_process_name(std::string const &short_name_suffix) {
  auto argc = processArguments.argc();
  auto *argv = processArguments.argv();

  third_party::setproctitle::SetProcessTitleFromCommandLine(argc, argv,
                                                            short_name_suffix);
}
}  // namespace dory::special
