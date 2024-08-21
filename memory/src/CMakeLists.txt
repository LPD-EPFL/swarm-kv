cmake_minimum_required(VERSION 3.9)
project(dorymemory CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

if(ENABLE_RETAINER)
  add_definitions(-DENABLE_RETAINER)
endif()

add_library(dorymemory ${HEADER_TIDER} locked-memory.cpp internal/heartbeat.cpp)
