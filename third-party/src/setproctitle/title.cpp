// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Define _GNU_SOURCE to ensure that <errno.h> defines
// program_invocation_short_name. Keep this at the top of the file since some
// system headers might include <errno.h> and the header could be skipped on
// subsequent includes.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "title.hpp"

#include <errno.h>  // Get program_invocation_short_name declaration.
#include <limits.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/prctl.h>
#include <unistd.h>

#include <string>

#include "internal/no_destructor.h"

// Linux/glibc doesn't natively have setproctitle().
#include "internal/set_process_title_linux.h"

#include "internal/utils.h"

namespace dory::third_party::setproctitle {

void SetProcessTitleFromCommandLine(int argc, const char** argv,
                                    std::string const& short_name_suffix) {
  // Build a single string which consists of all the arguments separated
  // by spaces. We can't actually keep them separate due to the way the
  // setproctitle() function works.
  std::string title;
  bool have_argv0 = false;

  // DCHECK_EQ(PlatformThread::CurrentId(), getpid());

  internal::setproctitle_init(argv);

  // In Linux we sometimes exec ourselves from /proc/self/exe, but this makes us
  // show up as "exe" in process listings. Read the symlink /proc/self/exe and
  // use the path it points at for our process title. Note that this is only for
  // display purposes and has no TOCTTOU security implications.
  std::string self_exe("/proc/self/exe");
  if (internal::readSymbolicLink(self_exe, title)) {
    have_argv0 = true;

    // If the binary has since been deleted, Linux appends " (deleted)" to the
    // symlink target. Remove it, since this is not really part of our name.
    const std::string kDeletedSuffix = " (deleted)";
    if (internal::endsWith(title, kDeletedSuffix))
      title.resize(title.size() - kDeletedSuffix.size());

    std::string base_name = internal::baseName(title);
    std::string short_name(base_name);

    base_name += short_name_suffix;

    // PR_SET_NAME is available in Linux 2.6.9 and newer.
    // When available at run time, this sets the short process name that shows
    // when the full command line is not being displayed in most process
    // listings.
    if (short_name_suffix.size() < 6) {
      auto overall_length = static_cast<int>(short_name.size()) +
                            static_cast<int>(short_name_suffix.size());

      unsigned erase_from_short_name =
          overall_length > 15 ? static_cast<unsigned>(overall_length - 15) : 0;

      short_name.erase(short_name.size() - erase_from_short_name);
      short_name += short_name_suffix;
    }

    prctl(PR_SET_NAME, short_name.c_str());

    // This prevents program_invocation_short_name from being broken by
    // setproctitle().
    static internal::NoDestructor<std::string> base_name_storage;
    *base_name_storage = std::move(base_name);
    program_invocation_short_name = &(*base_name_storage)[0];
  }

  for (auto i = 1; i < argc; ++i) {
    if (!title.empty()) title += " ";
    title += argv[i];
  }

  // Disable prepending argv[0] with '-' if we prepended it ourselves above.
  internal::setproctitle(have_argv0 ? "-%s" : "%s", title.c_str());
}
}  // namespace dory::third_party::setproctitle
