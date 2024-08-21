cmake_minimum_required(VERSION 3.10)
project(DoryConnectionTest CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

enable_testing()
include(GoogleTest)

add_executable(identifier_test identifier-test.cpp)
target_link_libraries(identifier_test ${CONAN_LIBS})
gtest_discover_tests(identifier_test)

add_executable(packing_test packing-test.cpp)
target_link_libraries(packing_test ${CONAN_LIBS})
gtest_discover_tests(packing_test)

add_executable(contexted_poller_test contexted-poller-test.cpp)
target_link_libraries(contexted_poller_test ${CONAN_LIBS})
gtest_discover_tests(contexted_poller_test)
