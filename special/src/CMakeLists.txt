cmake_minimum_required(VERSION 3.9)
project(doryspecial CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_library(doryspecial ${HEADER_TIDER} init-array.cpp proc-name.cpp
                        heartbeat.cpp)
