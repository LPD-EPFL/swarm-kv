#pragma once

#include <string>
#include <cstring>

#include <fcntl.h>           /* Definition of AT_* constants */
#include <unistd.h>
#include <libgen.h>



namespace dory::third_party::setproctitle::internal {
static bool readSymbolicLink(std::string const &symlink_path, std::string &target_path) {
  // DCHECK(!symlink_path.empty());
  // DCHECK(target_path);

  char buf[PATH_MAX];
  ssize_t count = readlink(symlink_path.c_str(), buf, sizeof(buf));

  bool error = count <= 0;

  if (error) {
    target_path.clear();
    return false;
  }

  for (ssize_t i = 0; i < count; i++) {
    target_path.push_back(buf[i]);
  }

  return true;
}

static bool endsWith(std::string const &fullString, std::string const &ending) {
    if (fullString.length() >= ending.length()) {
        return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}


static std::string baseName(std::string const &filePath) {
  auto len = filePath.size();
  char *copy = new char [len + 1];
  memcpy(copy, filePath.data(), len);
  copy[len] = 0;

  auto ret = std::string(basename(copy));
  delete []copy;

  return ret;
}
}