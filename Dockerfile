From ubuntu:focal

# Update container
RUN apt-get update && apt-get -y upgrade

# Install basic dependencies
RUN apt-get -y install libibverbs-dev libmemcached-dev python3 python3-pip cmake ninja-build clang-10 lld-10 gawk

# Fix the LLD path
# (If we install `lld` instead of `lld-10`, the following command is not needed)
RUN update-alternatives --install "/usr/bin/ld.lld" "ld.lld" "/usr/bin/ld.lld-10" 20

# Install extra dependencies
RUN apt-get -y install clang-format-10 clang-tidy-10 clang-tools-10 git

# Install essential python packages
RUN pip3 install --upgrade "conan>=1.47.0,<2.0"

# Install extra python packages
# pyyaml: Required from clang-tidy
# cmake-format: Required by format.sh
# black: Required by format.sh
# halo: Required by conan/invoker/invoker.py to show the compilation output compactly
RUN pip3 install --upgrade pyyaml"<6.0,>=3.11" cmake-format black halo

#RUN conan profile new default --detect
#RUN conan profile update settings.compiler.libcxx=libstdc++11 default

COPY . /dory/
RUN /dory/build.py
