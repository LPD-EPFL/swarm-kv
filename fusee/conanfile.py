#!/usr/bin/env python3

from conans import ConanFile, CMake


class DoryFuseeConan(ConanFile):
    name = "dory-fusee"
    version = "0.0.1"
    license = "MIT"
    # url = "TODO"
    description = "RDMA kv-store for spot VMs"
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
        "shared": [True, False],
        "log_level": ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "OFF"],
        "lto": [True, False],
    }
    default_options = {
        "shared": False,
        "log_level": "INFO",
        "dory-ctrl:log_level": "INFO",
        "dory-conn:log_level": "INFO",
        "dory-crypto:log_level": "INFO",
        "lto": True,
    }
    generators = "cmake"
    exports_sources = "src/*"
    python_requires = "dory-compiler-options/0.0.1@dory/stable"

    def configure(self):
        pass

    def requirements(self):
        self.requires("dory-conn/0.0.1")
        self.requires("dory-memstore/0.0.1")
        self.requires("dory-ctrl/0.0.1")
        self.requires("dory-shared/0.0.1")
        self.requires("dory-extern/0.0.1")
        self.requires("lyra/1.5.1")
        self.requires("hipony-enumerate/2.4.1")
        self.requires("xxhash/0.8.1")
        self.requires("fmt/7.1.3")

    def build(self):
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
        cmake.build()

    def deploy(self):
        self.copy("*", dst="bin", src="bin")

    def package(self):
        self.copy("*", dst="bin", src="bin")


if __name__ == "__main__":
    import os, pathlib, sys

    # Find dory root directory
    root_dir = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    while not os.path.isfile(os.path.join(root_dir, ".dory-root")):
        root_dir = root_dir.parent

    sys.path.append(os.path.join(root_dir, "conan", "invoker"))

    import invoker

    invoker.run()
