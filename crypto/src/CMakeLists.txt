cmake_minimum_required(VERSION 3.9)
project(dorycrypto CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_library(dorycrypto ${HEADER_TIDER} asymmetric/sodium.cpp
                       asymmetric/dalek.cpp hash/blake3.hpp)
