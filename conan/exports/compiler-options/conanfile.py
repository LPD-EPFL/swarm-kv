from conans import ConanFile
from compileroptions import *
import os, pathlib, shutil


def setup_cmake(build_folder):
    root_dir = os.path.dirname(os.path.abspath(__file__))

    setup_script = os.path.join(root_dir, "cmake", "setup.cmake")
    shutil.copy(setup_script, build_folder)


def set_options(cmake, strict=True):
    compiler = cmake.definitions["CONAN_COMPILER"].upper()
    build_type = cmake.definitions["CMAKE_BUILD_TYPE"].upper()

    cmake.definitions["CMAKE_C_FLAGS"] = " ".join(
        general(strict) + options["LANG"]["C"] + options["TARGET"][compiler]["C"]
    )
    cmake.definitions["CMAKE_CXX_FLAGS"] = " ".join(
        general(strict) + options["LANG"]["CXX"] + options["TARGET"][compiler]["CXX"]
    )
    cmake.definitions["CMAKE_CXX_FLAGS_{}".format(build_type)] = " ".join(
        options["BUILD_TYPE"][build_type]
    )
    cmake.definitions["CMAKE_C_FLAGS_{}".format(build_type)] = " ".join(
        options["BUILD_TYPE"][build_type]
    )
    cmake.definitions["CMAKE_EXE_LINKER_FLAGS"] = options["TARGET"][compiler]["LINKER"]


def lto_decision(cmake, lto_desired):
    if not lto_desired:
        return False

    compiler = cmake.definitions["CONAN_COMPILER"].upper()
    build_type = cmake.definitions["CMAKE_BUILD_TYPE"].upper()

    # [llvm-bugs] [Bug 38305] New: LTO doesn't allow optimization by size
    # http://lists.llvm.org/pipermail/llvm-bugs/2018-July/067046.html
    if compiler == "CLANG" and build_type == "MINSIZEREL":
        return False

    return True


def generator():
    # The default is None
    # return None
    # return "Unix Makefiles"
    return "Ninja"


class CompilerOptions(ConanFile):
    name = "dory-compiler-options"
    version = "0.0.1"
    exports = ["compileroptions.py", "cmake/setup.cmake"]
    url = "https://github.com/LPD-EPFL/dory"
    license = "MIT"
    description = "Compiler configuration package for Dory"
