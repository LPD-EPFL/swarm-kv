cmake_minimum_required(VERSION 3.9)
project(doryshared)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_library(doryshared ${HEADER_TIDER} host.cpp logger.cpp pinning.cpp)
