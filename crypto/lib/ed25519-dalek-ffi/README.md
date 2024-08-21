# Rust FFI for dalek-ed25519

## Install Rust

First, install the latest rust toolchain
```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then, switch to the appropriate rust version that is compatible with `dalek-ed25519`.
Version 1.50.0-nightly is tested to be working with `dalek-ed25519`.
```sh
rustup toolchain install nightly-2020-11-30 --profile minimal
rustup toolchain list
```

## Compile
Export on the following flags to compile with the AVX extension
```sh
# Requires nightly
export RUSTFLAGS="-C target_feature=+avx2" # to use avx2

# Or
export RUSTFLAGS="-C target_feature=+avx512ifma" # to use ifma
```

Run `make`
```sh
make
```

## Note
If compilation fails, remove `Cargo.lock` and try again.
