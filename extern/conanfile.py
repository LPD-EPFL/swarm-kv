#!/usr/bin/env python3

from conans import ConanFile, CMake


class DoryExtern(ConanFile):
    name = "dory-extern"
    version = "0.0.1"
    license = "MIT"
    description = "External header files"
    generators = "cmake"
    options = {"shared": [True, False]}
    default_options = {"shared": False}
    exports_sources = "src/*"
    python_requires = "dory-compiler-options/0.0.1@dory/stable"

    def build(self):
        self.python_requires["dory-compiler-options"].module.setup_cmake(
            self.build_folder
        )
        generator = self.python_requires["dory-compiler-options"].module.generator()
        cmake = CMake(self, generator=generator)
        cmake.configure(source_folder="src")

    def package(self):
        self.copy("*.hpp", dst="include/dory/extern", src="src")

    def package_info(self):
        self.cpp_info.system_libs = ["ibverbs", "memcached"]


if __name__ == "__main__":
    import os, pathlib, sys

    # Find dory root directory
    root_dir = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    while not os.path.isfile(os.path.join(root_dir, ".dory-root")):
        root_dir = root_dir.parent

    sys.path.append(os.path.join(root_dir, "conan", "invoker"))

    import invoker

    invoker.run()
