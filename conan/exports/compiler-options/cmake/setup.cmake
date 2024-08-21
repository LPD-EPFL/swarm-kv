macro(dory_disable_tider)
  if("$ENV{CLANG_GEN_TIDY_INFO}" STREQUAL "1")
    unset(ENV{CLANG_GEN_TIDY_INFO})
  endif()
endmacro()

macro(dory_setup_tider)
  if("$ENV{CLANG_GEN_TIDY_INFO}" STREQUAL "1")
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
  
    set(HEADER_TIDER_CNT 0)
  
    file(GLOB_RECURSE header_files "*.hpp")
    foreach(header_file ${header_files})
      file(RELATIVE_PATH header_file_path "${CMAKE_BINARY_DIR}"
           ${header_file})
  
      set(HEADER_TIDER_TMP "${CMAKE_BINARY_DIR}/tidy-${HEADER_TIDER_CNT}.cpp")
      list(APPEND HEADER_TIDER "${HEADER_TIDER_TMP}")
  
      file(WRITE ${HEADER_TIDER_TMP}
          "// This file was automatically generated. Do not edit!\n")
      file(WRITE ${HEADER_TIDER_TMP}
          "#ifndef DORY_TIDIER_ON\n#define DORY_TIDIER_ON\n#endif\n")
      file(APPEND ${HEADER_TIDER_TMP} "#include \"${header_file_path}\"\n")
  
      math(EXPR HEADER_TIDER_CNT "${HEADER_TIDER_CNT}+1")
    endforeach()
  endif()
endmacro()

macro(dory_setup_cmake)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT supported OUTPUT error)
  
  if(supported)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ${DORY_LTO})
  else()
    message(STATUS "IPO / LTO not supported: <${error}>")
  endif()
  
  dory_setup_tider()
  
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CONAN_SYSTEM_INCLUDES On)
  
  include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
  conan_basic_setup()
  
  if(SPDLOG_ACTIVE_LEVEL)
    add_definitions(-DSPDLOG_ACTIVE_LEVEL=${SPDLOG_ACTIVE_LEVEL})
  endif()
  
  message(STATUS "CMAKE_C_FLAGS: " ${CMAKE_C_FLAGS})
  message(STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS})
  message(STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE})
  message(STATUS "CMAKE_INTERPROCEDURAL_OPTIMIZATION: "
                 ${CMAKE_INTERPROCEDURAL_OPTIMIZATION})
endmacro()
