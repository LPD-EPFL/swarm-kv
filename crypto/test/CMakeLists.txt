cmake_minimum_required(VERSION 3.10)
project(DoryCryptoTest CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

enable_testing()
include(GoogleTest)

add_executable(cryptotest_dalek ${HEADER_TIDER} main.cpp)
set_target_properties(cryptotest_dalek PROPERTIES COMPILE_DEFINITIONS DALEK)
target_link_libraries(cryptotest_dalek ${CONAN_LIBS})
gtest_discover_tests(cryptotest_dalek)

add_executable(cryptotest_sodium ${HEADER_TIDER} main.cpp)
set_target_properties(cryptotest_sodium PROPERTIES COMPILE_DEFINITIONS SODIUM)
target_link_libraries(cryptotest_sodium ${CONAN_LIBS})
gtest_discover_tests(cryptotest_sodium)

add_executable(cryptotest_hash ${HEADER_TIDER} hash.cpp)
target_link_libraries(cryptotest_hash ${CONAN_LIBS})
gtest_discover_tests(cryptotest_hash)
