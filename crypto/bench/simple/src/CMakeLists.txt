cmake_minimum_required(VERSION 3.9)
project(cryptobench CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(cryptotest_dalek ${HEADER_TIDER} main.cpp)
set_target_properties(cryptotest_dalek PROPERTIES COMPILE_DEFINITIONS DALEK)
target_link_libraries(cryptotest_dalek ${CONAN_LIBS})

add_executable(cryptotest_sodium ${HEADER_TIDER} main.cpp)
set_target_properties(cryptotest_sodium PROPERTIES COMPILE_DEFINITIONS SODIUM)
target_link_libraries(cryptotest_sodium ${CONAN_LIBS})
