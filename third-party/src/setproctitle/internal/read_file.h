#pragma once

#include <string>
#include <fstream>
#include <streambuf>

namespace dory::third_party::setproctitle::internal {
static bool readFileToString(char const *filename, std::string &content) {
    std::ifstream fs(filename);

    for (auto it = std::istreambuf_iterator<char>(fs); it != std::istreambuf_iterator<char>(); it++) {
        content.push_back(*it);
    }
    
    return static_cast<bool>(fs);
}
}