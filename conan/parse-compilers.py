#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

import subprocess
import os
import argparse
import json

BUILD_TYPES = ["debug", "release", "relwithdebinfo", "minsizerel"]


def conan_profile(compiler_name, compiler_class):
    compiler_profile = compiler_name

    if compiler_name == compiler_class:
        with open(
            os.path.join(current_dir, "compilers", "{}-default".format(compiler_class)),
            "r",
        ) as default_version:
            compiler_profile = "{}-{}".format(
                compiler_class, int(default_version.read())
            )

    CONAN_PROFILE = os.path.join(
        current_dir, "profiles", "{}-{}.profile".format(compiler_profile, build_type)
    )

    COMPILER_PATH = None

    if compiler_name != compiler_class:
        COMPILER_PATH = os.path.join(current_dir, "compilers", compiler_name)

    return (CONAN_PROFILE, COMPILER_PATH)


current_dir = os.path.dirname(os.path.abspath(__file__))
available_compilers = subprocess.check_output(
    ["./compilers/link.sh"], cwd=current_dir, stderr=sys.stderr
)
available_compilers = available_compilers.decode("utf-8").split()

if len(available_compilers) == 0:
    print("No GCC or Clang compiler was detected!")
    print("Reason: `gcc` or `clang` executables do not exist!")
    exit(1)

if "gcc" in available_compilers:
    default_compiler = "gcc"
elif "clang" in available_compilers:
    default_compiler = "clang"
else:
    default_compiler = available_compilers[0]

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument(
    "-b",
    "--build-type",
    choices=BUILD_TYPES,
    default="release",
    nargs="?",
    dest="build_type",
    help="Type of build (default: release)",
)

descr_compiler = """Type of the compiler (default: {})
Possible options:
{}
""".format(
    default_compiler, "\n".join(available_compilers)
)

parser.add_argument(
    "-c",
    "--compiler",
    choices=available_compilers,
    default=default_compiler,
    nargs="?",
    dest="compiler",
    metavar="COMPILER",
    help=descr_compiler,
)

parser.add_argument(
    "-s",
    "--show",
    nargs="?",
    dest="build_type",
    help="Type of build (default: release)",
)

parser.add_argument(
    "--show-compilers",
    required=False,
    dest="compilers_list",
    help="Show list of available compilers",
    action="store_true",
)

parser.add_argument(
    "--show-builds",
    required=False,
    dest="build_types_list",
    help="Show list of available build profiles",
    action="store_true",
)

parser.add_argument(
    "--show-all-json",
    required=False,
    dest="show_all",
    help="Show list of available compilers and build profiles",
    action="store_true",
)

parser.add_argument(
    "--json",
    required=False,
    dest="json_output",
    help="Return configuration in a json file",
    action="store_true",
)

results = parser.parse_args()

compiler = results.compiler
build_type = results.build_type

if results.show_all:
    print(
        json.dumps(
            {
                "DEFAULT_COMPILER": default_compiler,
                "AVAILABLE_COMPILERS": available_compilers,
                "AVAILABLE_BUILD_TYPES": BUILD_TYPES,
            }
        )
    )
    exit(0)

if results.compilers_list:
    print(" ".join(available_compilers))
    exit(0)

if results.build_types_list:
    print(" ".join(BUILD_TYPES))
    exit(0)


def generate_config(compiler, json_output=False):
    def normalize_compiler_name(compiler):
        if compiler.startswith("gcc"):
            return "gcc", "g++"
        elif compiler.startswith("clang"):
            return "clang", "clang++"

    def complete_path(compiler_path):
        if compiler_path is None:
            return "$PATH"
        else:
            return "{}:$PATH".format(compiler_path)

    cc, cxx = normalize_compiler_name(compiler)

    CONAN_PROFILE, COMPILER_PATH = conan_profile(compiler, cc)

    if json_output:
        print(
            json.dumps(
                {
                    "CC": cc,
                    "CXX": cxx,
                    "PATH": COMPILER_PATH,
                    "CONAN_DEFAULT_PROFILE_PATH": CONAN_PROFILE,
                }
            )
        )
    else:
        print("CC={}; export CC;".format(cc))
        print("CXX={}; export CXX;".format(cxx))
        print("PATH={}; export PATH;".format(complete_path(COMPILER_PATH)))
        print(
            "CONAN_DEFAULT_PROFILE_PATH={}; export CONAN_DEFAULT_PROFILE_PATH;".format(
                CONAN_PROFILE
            )
        )


generate_config(compiler, results.json_output)
