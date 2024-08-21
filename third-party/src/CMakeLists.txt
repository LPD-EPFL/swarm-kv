cmake_minimum_required(VERSION 3.9)
project(dorythirdparty C CXX ASM)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_disable_tider()
dory_setup_cmake()

add_subdirectory(setproctitle)
add_subdirectory(mica)
add_subdirectory(blake3)
