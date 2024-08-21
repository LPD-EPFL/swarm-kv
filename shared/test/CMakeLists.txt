cmake_minimum_required(VERSION 3.10)
project(DorySharedTest CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

enable_testing()
include(GoogleTest)

add_executable(error_test error-test.cpp)
target_link_libraries(error_test ${CONAN_LIBS})
gtest_discover_tests(error_test)
