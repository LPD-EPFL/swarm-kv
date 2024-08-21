cmake_minimum_required(VERSION 3.9)
project(swarmkv CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(swarmkv ${HEADER_TIDER} main.cpp)
add_executable(raw_memory ${HEADER_TIDER} raw_memory_main.cpp)

target_link_libraries(swarmkv ${CONAN_LIBS})
target_link_libraries(raw_memory ${CONAN_LIBS})
