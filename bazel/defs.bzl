"""Cargo-derived Bazel targets for the standalone rebalancer crate."""

load("@crates//:data.bzl", "DEP_DATA")
load("@crates//:defs.bzl", "all_crate_deps", "crate_name", "edition")
load("@rules_rs//rs:rust_binary.bzl", "rust_binary")
load("@rules_rs//rs:rust_library.bzl", "rust_library")
load("@rules_rs//rs:rust_test.bzl", "rust_test")
load("@rules_rs_mutants//mutants:cargo_mutants_test.bzl", "cargo_mutants_test")
load("@rules_rust//rust:defs.bzl", "rust_doc", "rust_doc_test")
load("//tools/lint:linters.bzl", "clippy_test")

RUSTC_FLAGS = [
    "-Funsafe_code",
    # ConnectError's representation belongs to connectrpc-axum. Cargo reads
    # this allowance from the workspace lint table; Bazel needs it explicitly.
    "-Aclippy::result_large_err",
]

def _features():
    return DEP_DATA[native.package_name()]["crate_features"]

def _aliases(kinds):
    data = DEP_DATA[native.package_name()]
    labels = {}
    for kind in kinds:
        for dep in data.get(kind, []):
            labels[dep] = True
        for deps in data.get(kind + "_by_platform", {}).values():
            for dep in deps:
                labels[dep] = True
    return {label: name for label, name in data["aliases"].items() if label in labels}

def crate_library(name, deps = [], **kwargs):
    rust_library(
        name = name,
        srcs = native.glob(["src/**/*.rs"], exclude = ["src/bin/**"]),
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_name = crate_name(),
        edition = edition(),
        rustc_flags = RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True) + deps,
        **kwargs
    )
    clippy_test(name = name + "_clippy", srcs = [":" + name])
    rust_doc(name = name + "_doc", crate = ":" + name, rustdoc_flags = ["-Dwarnings"])

def crate_binary(name, crate_root, lib):
    rust_binary(
        name = name,
        srcs = [crate_root],
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_root = crate_root,
        edition = edition(),
        rustc_flags = RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True) + [lib],
    )
    rust_test(
        name = name + "_test",
        aliases = _aliases(["deps", "dev_deps"]),
        crate = ":" + name,
        crate_features = _features(),
        edition = edition(),
        rustc_flags = RUSTC_FLAGS,
        deps = all_crate_deps(normal_dev = True),
    )

def crate_tests(lib):
    rust_test(
        name = lib + "_test",
        aliases = _aliases(["deps", "dev_deps"]),
        crate = ":" + lib,
        crate_features = _features(),
        edition = edition(),
        rustc_flags = RUSTC_FLAGS,
        deps = all_crate_deps(normal_dev = True),
    )
    rust_doc_test(
        name = lib + "_doc_test",
        crate = ":" + lib,
        deps = all_crate_deps(normal_dev = True),
    )
    cargo_mutants_test(
        name = lib + "_mutants",
        jobs = 4,
        library = ":" + lib,
        shard_count = 8,
        tags = ["manual"],
        test = ":" + lib + "_test",
        timeout = "long",
    )
