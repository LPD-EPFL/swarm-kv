cmake_minimum_required(VERSION 3.9)
project(fusee CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(fusee ${HEADER_TIDER} main.cpp)

target_link_libraries(fusee ${CONAN_LIBS})
