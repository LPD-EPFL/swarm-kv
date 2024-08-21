#!/usr/bin/env python3

from conans import ConanFile, CMake


class DoryCryptoConan(ConanFile):
    name = "dory-crypto"
    version = "0.0.1"
    license = "MIT"
    # url = "TODO"
    description = "Crypto module"
    topics = "crypto"
    settings = {
        "os": None,
        "compiler": {
            "gcc": {"libcxx": "libstdc++11", "cppstd": ["17", "20"], "version": None},
            "clang": {"libcxx": "libstdc++11", "cppstd": ["17", "20"], "version": None},
        },
        "build_type": None,
        "arch": None,
    }
    options = {
        "isa": ["nosimd", "avx2"],
        "shared": [True, False],
        "log_level": ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "OFF"],
        "lto": [True, False],
    }
    default_options = {
        "isa": "nosimd",
        "shared": False,
        "log_level": "INFO",
        "lto": True,
    }
    generators = "cmake"
    exports_sources = (
        "src/*",
        "!lib/ed25519-dalek-ffi/*",
    )
    python_requires = "dory-compiler-options/0.0.1@dory/stable"

    def export_sources(self):
        # Copy the precompiled binaries in the build directory
        self.copy("lib/libed25519_dalek_ffi_nosimd.a")
        self.copy("lib/libed25519_dalek_ffi_nosimd.so")
        self.copy("lib/libed25519_dalek_ffi_avx2.a")
        self.copy("lib/libed25519_dalek_ffi_avx2.so")

    def _configure_cmake(self):
        self.python_requires["dory-compiler-options"].module.setup_cmake(
            self.build_folder
        )
        generator = self.python_requires["dory-compiler-options"].module.generator()
        cmake = CMake(self, generator=generator)

        self.python_requires["dory-compiler-options"].module.set_options(cmake)
        lto_decision = self.python_requires[
            "dory-compiler-options"
        ].module.lto_decision(cmake, self.options.lto)
        cmake.definitions["DORY_LTO"] = str(lto_decision).upper()
        cmake.definitions["SPDLOG_ACTIVE_LEVEL"] = "SPDLOG_LEVEL_{}".format(
            self.options.log_level
        )

        cmake.configure(source_folder="src")
        return cmake

    def configure(self):
        pass

    def requirements(self):
        self.requires("dory-shared/0.0.1")
        self.requires("dory-memstore/0.0.1")
        self.requires("dory-third-party/0.0.1")
        self.requires("libsodium/1.0.18")

    def build(self):
        cmake = self._configure_cmake()
        cmake.build()

    def package(self):
        self.copy("*.hpp", dst="include/dory/crypto/asymmetric", src="src/asymmetric")
        self.copy("*.hpp", dst="include/dory/crypto/hash", src="src/hash")
        self.copy("*.hpp", dst="include/dory/crypto/internal", src="src/internal")

        # Force only the static library, otherwise the .so object of the dalek ffi
        # will always be preferred (compared to the .a alternative)
        if self.options.shared:
            self.copy("*.so", dst="lib", src="lib", keep_path=False)
        else:
            self.copy("*.a", dst="lib", src="lib", keep_path=False)

    def package_info(self):
        if self.options.isa == "nosimd":
            self.cpp_info.libs = ["dorycrypto", "ed25519_dalek_ffi_nosimd"]
        else:
            self.cpp_info.libs = ["dorycrypto", "ed25519_dalek_ffi_avx2"]

        self.cpp_info.system_libs = ["dl"]  # Needed by rust ffi
        self.cpp_info.cxxflags = self.python_requires[
            "dory-compiler-options"
        ].module.get_cxx_options_for(self.settings.compiler, self.settings.build_type)


if __name__ == "__main__":
    import os, pathlib, sys

    # Find dory root directory
    root_dir = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    while not os.path.isfile(os.path.join(root_dir, ".dory-root")):
        root_dir = root_dir.parent

    sys.path.append(os.path.join(root_dir, "conan", "invoker"))

    import invoker

    invoker.run()
