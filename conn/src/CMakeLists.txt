cmake_minimum_required(VERSION 3.9)
project(doryconn CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_library(doryconn ${HEADER_TIDER} rc.cpp ud.cpp)

if(DORY_INTERNAL_TESTING)
  enable_testing()
  include(GoogleTest)

  add_executable(packing_test test/packing-test.cpp)
  target_link_libraries(packing_test ${CONAN_LIBS})
  gtest_discover_tests(packing_test)

endif()
