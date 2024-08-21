cmake_minimum_required(VERSION 3.9)
project(extern)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

find_library(IBVERBS_LIB ibverbs)
if(NOT IBVERBS_LIB)
  message(FATAL_ERROR "ibverbs library not found")
endif()

find_library(MEMCACHED_LIB memcached)
if(NOT MEMCACHED_LIB)
  message(FATAL_ERROR "memcached library not found")
endif()

if("$ENV{CLANG_GEN_TIDY_INFO}" STREQUAL "1")
  add_library(doryextern ${HEADER_TIDER})
endif()
