import sys

if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

import subprocess
import os
import inspect
import argparse
import sys
import pathlib
import json


def call_without_spinner(empty_text, *args, **kwargs):
    subprocess.run(*args, **kwargs, stdout=sys.stdout, stderr=sys.stderr, check=True)


try:
    from halo import Halo

    def call_with_spinner(spinner_text, *args, **kwargs):
        spinner = Halo(text=spinner_text, spinner="dots")
        spinner.start()
        proc = subprocess.Popen(
            *args, **kwargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            sys.stdout.write(stdout.decode("utf-8"))
            sys.stderr.write(stderr.decode("utf-8"))

            spinner.stop()
            spinner.fail()
            exit(proc.returncode)
        else:
            spinner.succeed()

    def warn(text):
        Halo(text_color="yellow").warn(text)

except ImportError:

    def call_with_spinner(*args, **kwargs):
        call_without_spinner(*args, **kwargs)

    def warn(text):
        print(text)


TIDY_DIR = ".clang-tidy-builds"


def exitOnFailure(func):
    def wrapper(*args, **kwargs):
        invocation = func(*args, **kwargs)
        if invocation.returncode != 0:
            exit(ret)

    return wrapper


def conan_profile(compiler_name, compiler_class, build_type):
    compiler_profile = compiler_name

    if compiler_name == compiler_class:
        with open(
            os.path.join(
                current_dir, "..", "compilers", "{}-default".format(compiler_class)
            ),
            "r",
        ) as default_version:
            compiler_profile = "{}-{}".format(
                compiler_class, int(default_version.read())
            )

    CONAN_PROFILE = os.path.join(
        current_dir,
        "..",
        "profiles",
        "{}-{}.profile".format(compiler_profile, build_type),
    )

    COMPILER_PATH = None

    if compiler_name != compiler_class:
        COMPILER_PATH = os.path.join(current_dir, "..", "compilers", compiler_name)

    return (CONAN_PROFILE, COMPILER_PATH)


called_directly = "DORY_INVOKED_FROM_BUILDSCRIPT" not in os.environ

current_dir = os.path.dirname(os.path.abspath(__file__))

root_dir = pathlib.Path(current_dir)
while not os.path.isfile(os.path.join(root_dir, ".dory-root")):
    root_dir = root_dir.parent

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

if called_directly:
    available_options = json.loads(
        subprocess.check_output(
            [os.path.join(root_dir, "build.py"), "--available-options"],
            cwd=current_dir,
            stderr=subprocess.DEVNULL,
        )
    )

    available_compilers = available_options["AVAILABLE_COMPILERS"]
    default_compiler = available_options["DEFAULT_COMPILER"]
    available_build_types = available_options["AVAILABLE_BUILD_TYPES"]

    if len(available_compilers) == 0:
        print("No GCC or Clang compiler was detected!")
        print("Reason: `gcc` or `clang` executables do not exist!")
        exit(1)

    parser.add_argument(
        "-b",
        "--build-type",
        choices=available_build_types,
        default="release",
        nargs="?",
        dest="build_type",
        help="Type of build (default: release)",
    )

    descr_compiler = "Type of the compiler (default: {})\nPossible options:\n{}".format(
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
else:
    parser.add_argument(
        "-b",
        "--build-type",
        nargs="?",
        dest="build_type",
    )

    parser.add_argument(
        "-c",
        "--compiler",
        nargs="?",
        dest="compiler",
    )


parser.add_argument(
    "--verbose",
    required=False,
    dest="verbose",
    help="Show verbose information",
    action="store_true",
)

parser.add_argument(
    "--name-only",
    required=False,
    dest="name_only",
    help="Show conan package information",
    action="store_true",
)

parser.add_argument(
    "--build-locally",
    required=False,
    dest="build_locally",
    help="Build the package locally",
    action="store_true",
)

parser.add_argument(
    "--export-only",
    required=False,
    dest="export_only",
    help="Export the package (useful for packages with prebuilt/precompiled objects/libraries)",
    action="store_true",
)

parser.add_argument(
    "--gen-tidy-info",
    required=False,
    dest="gen_tidy_info",
    help="Generate information to be used by clang-tidy",
    action="store_true",
)

parser.add_argument(
    "--test-package",
    required=False,
    dest="test_package",
    help="Run package-level tests",
    action="store_true",
)

parser.add_argument(
    "--remove-package",
    required=False,
    dest="remove_package",
    help="Remove package from Conan cache",
    action="store_true",
)

parser.add_argument(
    "--check-changes",
    required=False,
    dest="check_changes",
    help="Check if the package has changed",
    action="store_true",
)


def generate_config(compiler, build_type):
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

    CONAN_PROFILE, COMPILER_PATH = conan_profile(compiler, cc, build_type)

    return {
        "CC": cc,
        "CXX": cxx,
        "PATH": COMPILER_PATH,
        "CONAN_DEFAULT_PROFILE_PATH": CONAN_PROFILE,
    }


def set_environment(env, config):
    # If CONAN_DEFAULT_PROFILE_PATH is set, we skip
    # the environment configuration, which should have
    # been set by build.py
    if "CONAN_DEFAULT_PROFILE_PATH" in env:
        return

    conf = config.copy()

    # Prepend the path and set the rest
    if conf["PATH"]:
        if "PATH" not in env or not env["PATH"].strip():
            env["PATH"] = conf["PATH"]
        else:
            env["PATH"] = conf["PATH"] + os.pathsep + env["PATH"]

    del conf["PATH"]
    env.update(conf)


def get_cfp(real=False):
    # Get the second stack frame
    frame = inspect.stack()[2]
    p = frame[0].f_code.co_filename
    return p


def get_classname(module_name):
    def filter_obj(obj):
        return inspect.isclass(obj) and obj.__module__ == module_name

    clsmembers = inspect.getmembers(sys.modules[module_name], filter_obj)
    if len(clsmembers) != 1:
        raise NameError("Support for only one Conan class per conanfile.py")

    clsmember = clsmembers[0][1]
    if not clsmember.__name__.startswith("Dory"):
        raise NameError("Conan class name must start with `Dory`")

    return (getattr(clsmember, "name", None), getattr(clsmember, "version", None))


def tidy(call, spinner_text, env, working_dir, destination_dir):
    dest_dir = os.path.join(current_dir, "..", "..", TIDY_DIR, destination_dir)
    call(
        spinner_text,
        "{} {}".format(os.path.join(current_dir, "build-tidy.sh"), dest_dir),
        cwd=working_dir,
        shell=True,
        env=env,
    )


def run(doNothing=False, buildable=True, tidyable=True, outOfTree=False):
    results = parser.parse_args()

    if doNothing:
        return

    if results.verbose:
        call = call_without_spinner
    else:
        call = call_with_spinner

    env = os.environ.copy()

    conanfile_dir = os.path.dirname(get_cfp())
    test_conanfile_dir = os.path.join(conanfile_dir, "test")

    name, version = get_classname("__main__")
    if not name.startswith("dory-"):
        raise NameError("Conan class name attribute must start with `dory-`")

    if results.name_only:
        print("{}/{}".format(name, version))
        return

    if results.remove_package:
        name, version = get_classname("__main__")
        if name and version:
            subprocess.run(
                "conan remove '{}'".format("/".join([name, version])),
                cwd=conanfile_dir,
                stdout=sys.stdout,
                stderr=sys.stderr,
                shell=True,
                check=True,
                env=env,
            )

        return

    if results.check_changes:
        name, version = get_classname("__main__")
        if name and version:
            subprocess.run(
                os.path.join(current_dir, "check-changes.sh")
                + " "
                + os.path.join(
                    root_dir,
                    ".deps",
                    "exports",
                    "{}.conanbuild".format(name.replace("dory-", "")),
                ),
                cwd=conanfile_dir,
                stdout=sys.stdout,
                stderr=sys.stderr,
                shell=True,
                check=True,
                env=env,
            )

        return

    compiler = results.compiler
    build_type = results.build_type
    env_config = generate_config(compiler, build_type)
    set_environment(env, env_config)

    if results.export_only:
        call(
            "Exporting {}/{} locally [{}, {}]".format(
                name, version, compiler, build_type
            ),
            os.path.join(current_dir, "export-only.sh"),
            cwd=conanfile_dir,
            shell=True,
            env=env,
        )

        return

    direct_tidying = False
    if called_directly:
        valid = False
        if buildable and results.build_locally:
            valid = True
            if not outOfTree:
                cmd = [
                    os.path.join(root_dir, "build.py"),
                    "-c",
                    compiler,
                    "-b",
                    build_type,
                    "--deps-only",
                    name[len("dory-") :],
                ]
                subprocess.run(
                    " ".join(cmd),
                    cwd=conanfile_dir,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                    shell=True,
                    check=True,
                )
            else:
                warn(
                    "Building out-of-tree! Dependent dory packages need to be installed manually and they are not re-packaged when changed"
                )

            call(
                "Building {}/{} locally [{}, {}]".format(
                    name, version, compiler, build_type
                ),
                os.path.join(current_dir, "build-locally.sh"),
                cwd=conanfile_dir,
                shell=True,
                env=env,
            )
        if tidyable and results.gen_tidy_info:
            direct_tidying = True
            valid = True

        if not valid:
            print("Some parameters cannot be called directly")
            exit(1)

    if direct_tidying or results.gen_tidy_info:
        if not tidyable:
            return

        dest_dir_name = os.path.relpath(conanfile_dir, root_dir)
        dest_dir_name = dest_dir_name.replace(os.path.sep, "_")
        dest_dir_name = dest_dir_name.replace(".", "_")

        # Mangle destination name
        dest_dir_name += "_"

        if results.test_package and os.path.isdir(test_conanfile_dir):
            dest_dir_name = os.path.relpath(test_conanfile_dir, root_dir)
            dest_dir_name = dest_dir_name.replace(os.path.sep, "_")
            dest_dir_name = dest_dir_name.replace(".", "_")

            # Mangle destination name
            dest_dir_name += "_"

            tidy(
                call,
                "Generating tidy info for {}/{} test-package [{}, {}]".format(
                    name, version, compiler, build_type
                ),
                env,
                test_conanfile_dir,
                dest_dir_name,
            )

        else:
            tidy(
                call,
                "Generating tidy info for {}/{} package [{}, {}]".format(
                    name, version, compiler, build_type
                ),
                env,
                conanfile_dir,
                dest_dir_name,
            )

        return

    if called_directly:
        return

    if buildable:
        if results.test_package:
            if os.path.isdir(test_conanfile_dir):
                call(
                    "Testing package {}/{} [{}, {}]".format(
                        name, version, compiler, build_type
                    ),
                    os.path.join(current_dir, "build-lib-test.sh")
                    + " "
                    + os.path.join(
                        root_dir,
                        ".deps",
                        compiler,
                        build_type,
                        "{}.conantest".format(name.replace("dory-", "")),
                    ),
                    cwd=conanfile_dir,
                    shell=True,
                    env=env,
                )
        else:
            call(
                "Building package {}/{} [{}, {}]".format(
                    name, version, compiler, build_type
                ),
                os.path.join(current_dir, "build-lib-notest.sh")
                + " "
                + os.path.join(
                    root_dir,
                    ".deps",
                    compiler,
                    build_type,
                    "{}.conandep".format(name.replace("dory-", "")),
                ),
                cwd=conanfile_dir,
                shell=True,
                env=env,
            )
