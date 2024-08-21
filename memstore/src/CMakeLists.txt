cmake_minimum_required(VERSION 3.9)
project(dorymemstore CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_library(dorymemstore ${HEADER_TIDER} store.cpp)
