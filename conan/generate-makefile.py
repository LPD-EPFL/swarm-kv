#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

import subprocess
import os
import argparse
import json
import pathlib
import yaml
import itertools
import textwrap

from collections import defaultdict

# From https://www.geeksforgeeks.org/detect-cycle-in-a-graph/
class Graph:
    def __init__(self):
        self.graph = {}
        self.done = False

    def addEdges(self, u, edges):
        if self.done:
            raise Exception("Cannot add any more edges")
        self.graph[u] = []
        self.graph[u].extend(edges)

    def doneAdding(self):
        self.V = len(self.graph.keys())

        self.translator = {}
        self.inverse_translator = {}
        for idx, name in enumerate(self.graph.keys()):
            self.translator[name] = idx
            self.inverse_translator[idx] = name

        new_graph = self.graph
        self.graph = {}
        for v, ns in new_graph.items():
            self.addEdges(self.translator[v], [self.translator[n] for n in ns])

        self.done = True

    def isCyclicUtil(self, v, visited, recStack):

        # Mark current node as visited and
        # adds to recursion stack
        visited[v] = True
        recStack[v] = True

        # Recur for all neighbours
        # if any neighbour is visited and in
        # recStack then graph is cyclic
        for neighbour in self.graph[v]:
            if visited[neighbour] == False:
                if self.isCyclicUtil(neighbour, visited, recStack) == True:
                    return True
            elif recStack[neighbour] == True:
                return True

        # The node needs to be poped from
        # recursion stack before function ends
        recStack[v] = False
        return False

    # Returns true if graph is cyclic else false
    def firstCycle(self):
        if not self.done:
            raise Exception("Finish adding edges")

        visited = [False] * self.V
        recStack = [False] * self.V
        for node in range(self.V):
            if visited[node] == False:
                if self.isCyclicUtil(node, visited, recStack) == True:
                    return self.inverse_translator[node]
        return None


root_dir = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
while not os.path.isfile(os.path.join(root_dir, ".dory-root")):
    root_dir = root_dir.parent

available_config = json.loads(
    subprocess.check_output(
        ["./conan/parse-compilers.py", "--show-all-json"],
        cwd=root_dir,
        stderr=sys.stderr,
    )
)
default_compiler = available_config["DEFAULT_COMPILER"]
available_compilers = available_config["AVAILABLE_COMPILERS"]
available_build_types = available_config["AVAILABLE_BUILD_TYPES"]

targets = yaml.load(
    open(os.path.join(root_dir, "targets.yaml")), Loader=yaml.FullLoader
)


def separateTargets(targets):
    packages = dict()
    tests = dict()

    for target_name, target_deps in targets.items():
        if target_name.startswith("test-"):
            tests[target_name] = target_deps
        else:
            packages[target_name] = target_deps

    return packages, tests


def pathTransformation(package, test):
    if test:
        conanfile_location = os.path.join(".jenkins", "tests")

        def unmangler(name):
            if name.startswith("test-"):
                return name[len("test-") :]
            return name

    else:
        conanfile_location = ""

        def unmangler(name):
            return name

    return os.path.join(root_dir, conanfile_location, unmangler(package))


def checkTargets(targets):
    def hasDuplicates(targets):
        for target, deps in targets.items():
            if len(deps) != len(set(deps)):
                print("Target `{}` has duplicates in its dependencies".format(target))
                return True

        return False

    def hasUndefinedDependency(targets):
        all_deps = set([dep for deps in targets.values() for dep in deps])
        all_targets = set(targets.keys())
        diff = all_deps.difference(all_targets)
        if len(diff) > 0:
            print("Dependencies `{}` do not exist as targets".format(", ".join(diff)))
            return True

        return False

    def isNotPackage(targets):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = pathlib.Path(current_dir)
        while not os.path.isfile(os.path.join(root_dir, ".dory-root")):
            root_dir = root_dir.parent

        for target in targets.keys():
            path_prefix = (
                os.path.join(".jenkins", "tests", target[len("test-") :])
                if target.startswith("test-")
                else target
            )
            file_path = os.path.join(root_dir, path_prefix, "conanfile.py")
            if not os.path.isfile(file_path):
                print("Invalid target: file `{}` does not exist".format(file_path))
                return True

            if not os.access(file_path, os.X_OK):
                print("Invalid target: file `{}` does not executable".format(file_path))
                return True

        return False

    if hasDuplicates(targets):
        exit(1)

    if hasUndefinedDependency(targets):
        exit(1)

    if isNotPackage(targets):
        exit(1)

    g = Graph()
    for target, deps in targets.items():
        g.addEdges(target, deps)

    g.doneAdding()

    cycle = g.firstCycle()
    if cycle:
        print("Target `{}` forms a dependency cycle".format(cycle))
        exit(1)


def prelude(f):
    print(
        textwrap.dedent(
            """
    ifneq ($(DORY_BUILD_VERBOSITY),)
        SILENCE =
        VERBOSITY = "--verbose"
    else
        SILENCE = @
        VERBOSITY = 
    endif"""
        ),
        file=f,
    )

    print("FORCE:\n", file=f)
    print(
        'compiler-options.check: FORCE\n\t$(SILENCE)cd "{}" && conan export . dory/stable > /dev/null'.format(
            os.path.join(root_dir, "conan", "exports", "compiler-options")
        ),
        file=f,
    )


def createConanPackagesCheckFiles(f, targets, test=False):
    packages = targets.keys()
    for package in targets.keys():
        print(
            '{}.check: FORCE\n\t$(SILENCE)cd "{}" && ./conanfile.py --check-changes'.format(
                package, pathTransformation(package, test)
            ),
            file=f,
        )


def createConanPackagesBuildFiles(f, targets, test=False):
    packages = targets.keys()

    for compiler, build_type, package in itertools.product(
        available_compilers, available_build_types, packages
    ):
        print(
            ".deps/{}/{}/{}.conandep : .deps/exports/{}.conanbuild {}\n\t{}".format(
                compiler,
                build_type,
                package,
                package,
                " ".join(
                    map(
                        lambda p: ".deps/{}/{}/{}.conandep".format(
                            compiler, build_type, p
                        ),
                        targets[package],
                    )
                ),
                '$(SILENCE)cd "{}" && ./conanfile.py $(VERBOSITY) -c {} -b {}'.format(
                    pathTransformation(package, test), compiler, build_type
                ),
            ),
            file=f,
        )

        # Tidy on packages runs every time it is invoked
        print(
            ".deps/{0}/{1}/{2}.conantidy : .deps/{0}/{1}/{2}.conandep\n\t{3}".format(
                compiler,
                build_type,
                package,
                '$(SILENCE)cd "{}" && ./conanfile.py $(VERBOSITY) -c {} -b {} --gen-tidy-info'.format(
                    pathTransformation(package, test), compiler, build_type
                ),
            ),
            file=f,
        )

        # Tests run every time they are invoked
        print(
            ".deps/{0}/{1}/{2}.conantest : .deps/{0}/{1}/{2}.conandep\n\t{3}".format(
                compiler,
                build_type,
                package,
                '$(SILENCE)cd "{}" && ./conanfile.py $(VERBOSITY) -c {} -b {} --test-package'.format(
                    pathTransformation(package, test), compiler, build_type
                ),
            ),
            file=f,
        )

        # Tidy on tests runs every time it is invoked
        print(
            ".deps/{0}/{1}/{2}.conantesttidy : .deps/{0}/{1}/{2}.conantest\n\t{3}".format(
                compiler,
                build_type,
                package,
                '$(SILENCE)cd "{}" && ./conanfile.py $(VERBOSITY) -c {} -b {} --gen-tidy-info --test-package'.format(
                    pathTransformation(package, test), compiler, build_type
                ),
            ),
            file=f,
        )


checkTargets(targets)

f = open(sys.argv[1], "w")

prelude(f)
package_targets, test_targets = separateTargets(targets)

createConanPackagesCheckFiles(f, package_targets)
createConanPackagesBuildFiles(f, package_targets)

createConanPackagesCheckFiles(f, test_targets, test=True)
createConanPackagesBuildFiles(f, test_targets, test=True)
