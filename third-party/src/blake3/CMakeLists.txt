add_library(
  dorythirdpartyblake3
  blake3.c
  internal/blake3_dispatch.c
  internal/blake3_portable.c
  internal/blake3_sse2_x86-64_unix.S
  internal/blake3_sse41_x86-64_unix.S
  internal/blake3_avx2_x86-64_unix.S
  internal/blake3_avx512_x86-64_unix.S)

target_link_libraries(dorythirdpartyblake3 ${CONAN_LIBS})
