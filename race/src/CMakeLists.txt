cmake_minimum_required(VERSION 3.9)
project(doryrace CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(race_server ${HEADER_TIDER} server.cpp)
target_link_libraries(race_server ${CONAN_LIBS})

add_executable(race_test_client ${HEADER_TIDER} test_client.cpp)
target_link_libraries(race_test_client ${CONAN_LIBS})
