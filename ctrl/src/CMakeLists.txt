cmake_minimum_required(VERSION 3.9)
project(doryctrl CXX)

if(DORY_CTRL_DM)
  # Remove all flags, as Werror causes the test program to fail
  set(CMAKE_CXX_FLAGS_ORIG ${CMAKE_CXX_FLAGS})
  set(CMAKE_CXX_FLAGS "")

  include(CheckSymbolExists)
  check_symbol_exists(ibv_alloc_dm "infiniband/verbs.h" HAVE_DEV_MEM)

  if(NOT HAVE_DEV_MEM)
    message(
      FATAL_ERROR "DeviceMemory is not supported by the current libibverbs.")
  endif()

  add_definitions(-DDORY_CTRL_DM)
  set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS_ORIG})
endif()

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_library(doryctrl ${HEADER_TIDER} device.cpp block.cpp)
