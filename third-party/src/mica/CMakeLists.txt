add_library(dorythirdpartymica internal/mica.c internal/city.c)

target_link_libraries(dorythirdpartymica ${CONAN_LIBS})
