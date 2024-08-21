add_library(dorythirdpartysetproctitle title.cpp
                                       internal/set_process_title_linux.cpp)

target_link_libraries(dorythirdpartysetproctitle ${CONAN_LIBS})
