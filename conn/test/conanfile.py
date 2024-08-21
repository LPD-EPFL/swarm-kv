import os

from conans import ConanFile, CMake, tools


class ConnTestConan(ConanFile):
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
        "fPIC": [True, False],
        "lto": [True, False],
        "log_level": ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "OFF"],
    }
    default_options = {"shared": False, "fPIC": True, "lto": True, "log_level": "INFO"}
    generators = "cmake"
    exports_sources = "src/*"
    python_requires = "dory-compiler-options/0.0.1@dory/stable"

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

        cmake.configure()
        cmake.build()

    def requirements(self):
        self.requires("gtest/1.10.0")
        self.requires("dory-conn/0.0.1")

    def imports(self):
        self.copy("*.so*", dst="bin", src="lib")

    def test(self):
        if not tools.cross_building(self):
            self.run("CTEST_OUTPUT_ON_FAILURE=1 GTEST_COLOR=1 ctest")
