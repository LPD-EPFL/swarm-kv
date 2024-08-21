cmake_minimum_required(VERSION 3.9)
project(dorycryptobench CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(cryptobench ${HEADER_TIDER} main.cpp)

target_link_libraries(cryptobench ${CONAN_LIBS})
