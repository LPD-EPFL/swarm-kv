#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

import subprocess
import os
import json
import functools
import argparse
import yaml
import itertools


def detectTarget(current_dir, special_targets, normal_targets, test_targets):
    all_targets = special_targets + normal_targets + test_targets

    def removePrefix(text, prefix):
        if text.startswith(prefix):
            return text[len(prefix) :]
        else:
            return text

    def check(target):
        # First check if target is symbolic
        if target in all_targets:
            return target

        # For non-symbolic targets, try to match with the path
        target_dir = os.path.join(os.getcwd(), target)
        if os.path.isdir(target_dir):
            for normal in normal_targets:
                normal_target_dir = os.path.join(current_dir, normal)
                if os.path.samefile(target_dir, normal_target_dir):
                    return normal

            for test in test_targets:
                unprefixed_test = removePrefix(test, "test-")
                test_target_dir = os.path.join(
                    current_dir, os.path.join(".jenkins", "tests"), unprefixed_test
                )
                if os.path.samefile(target_dir, test_target_dir):
                    return test

        return None

    return check


class TargetNormalizer:
    def __init__(self):
        self.targets_contain_creative = False
        self.targets_contain_destructive = False

    def run(self, targetCheck, creative_set, destructive_set, normal_set):
        def allowed(target):
            normalized = targetCheck(target)
            if normalized:
                if normalized in creative_set or normalized in normal_set:
                    self.targets_contain_creative = True
                    if self.targets_contain_destructive:
                        raise argparse.ArgumentTypeError(
                            "invalid choice: {} (mix of creative and destructive targets)".format(
                                normalized
                            )
                        )

                if normalized in destructive_set:
                    self.targets_contain_destructive = True
                    if self.targets_contain_creative:
                        raise argparse.ArgumentTypeError(
                            "invalid choice: {} (mix of creative and destructive targets)".format(
                                normalized
                            )
                        )

                return normalized

            targetList = list(creative_set) + list(destructive_set) + list(normal_set)
            args = {"value": target, "choices": ", ".join(map(repr, targetList))}
            msg = "invalid choice: %(value)r (choose from %(choices)s)"
            raise argparse.ArgumentTypeError(msg % args)

        return allowed


target_normalizer = TargetNormalizer()

current_dir = os.path.dirname(os.path.abspath(__file__))

available_config = json.loads(
    subprocess.check_output(
        ["./conan/parse-compilers.py", "--show-all-json"],
        cwd=current_dir,
        stderr=sys.stderr,
    )
)
default_compiler = available_config["DEFAULT_COMPILER"]
available_compilers = available_config["AVAILABLE_COMPILERS"]
available_build_types = available_config["AVAILABLE_BUILD_TYPES"]

normal_targets = {}
test_targets = {}
special_creative_targets = {"all": [], "tests": [], "everything": []}
special_destructive_targets = {
    "clean": [],
    "buildclean": [],
    "tidyclean": [],
    "distclean": [],
}
special_targets = {**special_creative_targets, **special_destructive_targets}

target_dict = yaml.load(
    open(os.path.join(current_dir, "targets.yaml")), Loader=yaml.FullLoader
)

for target_name, target_deps in target_dict.items():
    if target_name.startswith("test-"):
        test_targets[target_name] = target_deps
    else:
        normal_targets[target_name] = target_deps


def sorted_targets(targets):
    return sorted(targets.keys())


def flatten(lst):
    return functools.reduce(lambda a, b: a + b, lst, [])


make_list = list(map(sorted_targets, [special_targets, normal_targets, test_targets]))

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument(
    "--verbose",
    required=False,
    dest="verbose",
    help="Show verbose compilation information",
    action="store_true",
)

parser.add_argument(
    "--serial-build",
    required=False,
    dest="serial",
    help="Build one package at a time",
    action="store_true",
)

parser.add_argument(
    "-b",
    "--build-type",
    choices=available_build_types,
    default="relwithdebinfo",
    nargs="?",
    dest="build_type",
    help="Type of build (default: relwithdebinfo)",
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
    "--available-options",
    required=False,
    dest="available_options",
    action="store_true",
    help=argparse.SUPPRESS,
)

parser.add_argument(
    "--deps-only",
    required=False,
    dest="deps_only",
    action="store_true",
    help="Consider as target(s) the dependencies of the specified target(s)",
)

descr_target = """What targets to build (default: all)
Possible targets:
{}
""".format(
    "\n\n".join(["\n".join(make_list_group) for make_list_group in make_list])
)

parser.add_argument(
    "TARGET",
    default="all",
    nargs="*",
    # choices=flatten(make_list),
    metavar="TARGET",
    type=target_normalizer.run(
        detectTarget(
            current_dir,
            sorted_targets(special_targets),
            sorted_targets(normal_targets),
            sorted_targets(test_targets),
        ),
        set(special_creative_targets.keys()),
        set(special_destructive_targets.keys()),
        set(normal_targets.keys()).union(set(test_targets.keys())),
    ),
    help=descr_target,
)

parser.add_argument(
    "--gen-tidy-info",
    required=False,
    dest="gen_tidy_info",
    help="Generate information to be used by clang-tidy",
    action="store_true",
)

parser.add_argument(
    "--test-packages",
    required=False,
    dest="test_packages",
    help="Run the unit-test suite for packages",
    action="store_true",
)

results = parser.parse_args()

if results.available_options:
    print(
        json.dumps(
            {
                "DEFAULT_COMPILER": default_compiler,
                "AVAILABLE_COMPILERS": available_compilers,
                "AVAILABLE_BUILD_TYPES": available_build_types,
            }
        )
    )
    exit(0)

compiler = results.compiler
build_type = results.build_type

if isinstance(results.TARGET, str):
    targets = [results.TARGET]
else:
    targets = list(results.TARGET)

# Remove duplicates
targets = list(set(targets))

env = os.environ.copy()

env["DORY_INVOKED_FROM_BUILDSCRIPT"] = "1"
make_opt = []
make_opt_stderr = None

if not results.serial:
    make_opt.append("-j")

if not results.verbose:
    make_opt.append("-s")
    make_opt_stderr = subprocess.DEVNULL
else:
    env["DORY_BUILD_VERBOSITY"] = "1"

if not set(special_destructive_targets.keys()).isdisjoint(set(targets)):
    cmd_destructive_targets = "make {} -C conan {}".format(
        " ".join(make_opt), " ".join(targets)
    )
    ret = subprocess.call(
        cmd_destructive_targets,
        cwd=current_dir,
        env=env,
        shell=True,
    )

    exit(ret)

if "all" in targets:
    targets.remove("all")
    targets.extend(normal_targets.keys())

if "tests" in targets:
    targets.remove("tests")
    targets.extend(test_targets.keys())

if "everything" in targets:
    targets.remove("everything")
    targets.extend(normal_targets.keys())
    targets.extend(test_targets.keys())

# Remove duplicates again
targets = list(set(targets))

if results.deps_only:
    targets = list(set(flatten([target_dict[target] for target in targets])))

config = json.loads(
    subprocess.check_output(
        ["./conan/parse-compilers.py", "-c", compiler, "-b", build_type, "--json"],
        cwd=current_dir,
        stderr=subprocess.DEVNULL,
    )
)

CC = config["CC"]
env["CC"] = CC

CXX = config["CXX"]
env["CXX"] = CXX

COMPILER_PATH = config["PATH"]
if COMPILER_PATH:
    env["PATH"] = "{}:".format(COMPILER_PATH) + env["PATH"]


def convert_target(target, compiler, buildtype, gen_tidy, test_pkg):
    prefix = os.path.join(".deps", compiler, buildtype)

    if gen_tidy and test_pkg:
        return os.path.join(prefix, "{}.conantesttidy".format(target))

    if gen_tidy and not test_pkg:
        return os.path.join(prefix, "{}.conantidy".format(target))

    if not gen_tidy and test_pkg:
        return os.path.join(prefix, "{}.conantest".format(target))

    return os.path.join(prefix, "{}.conandep".format(target))


def check_targets(targets):
    def gather_check_targets(target, store):
        if not target:
            return

        store.add(target)
        [gather_check_targets(dep, store) for dep in target_dict[target]]

    checks = set(["compiler-options"])
    [gather_check_targets(target, checks) for target in targets]
    return list(checks)


cmd_check_target_changes = "make {} -C conan".format(" ".join(make_opt))
ret = subprocess.call(
    cmd_check_target_changes,
    cwd=current_dir,
    stderr=make_opt_stderr,
    env=env,
    shell=True,
)
if ret > 0:
    exit(ret)

cmd_check_package_changes = "make {} -f .deps/Makefile.generated {}".format(
    " ".join(make_opt),
    " ".join(map(lambda trgt: "{}.check".format(trgt), check_targets(targets))),
)
cmd_build = "make {} -f .deps/Makefile.generated {}".format(
    " ".join(make_opt),
    " ".join(
        map(
            lambda trgt: convert_target(
                trgt,
                results.compiler,
                results.build_type,
                results.gen_tidy_info,
                results.test_packages,
            ),
            targets,
        )
    ),
)

# The compilers need to be explicity set, as conan won't interfere with the
# build system. Therefore, make sure the CC/CXX flags match the conan profile.
ret = subprocess.call(
    "{} && {}".format(
        cmd_check_package_changes,
        cmd_build,
    ),
    cwd=current_dir,
    env=env,
    shell=True,
)

exit(ret)
