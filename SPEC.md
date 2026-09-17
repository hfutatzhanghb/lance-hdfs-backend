# lance-hdfs-backend Specification

Status: Development against upstream Lance main
Date: 2026-09-17
Author: Codex
Target repository: `hfutatzhanghb/lance-hdfs-backend`
Target crate: `lance-hdfs-backend`

## 1. Problem

当前项目基于官方 Lance main 分支开发，提供独立的 HDFS provider，让使用 Lance 的应用可以通过：

```text
hdfs://<name-node-or-nameservice>/<path>
```

读写 Lance dataset。

该 crate 的目标是把本地
`/Users/admin/IdeaProjects/lance-bzl`
仓库中 `v8.0.0-bzl` 分支的 HDFS feature 提取为独立 crate。该分支仅作为历史行为参考；当前依赖使用官方 Lance main。待兼容的 Lance 版本发布到 crates.io 后，再准备本项目的 crates.io 发布。

## 2. Reference Inputs

### 2.1 Local Lance Reference

Verified from local repository:

- Repository: `/Users/admin/IdeaProjects/lance-bzl`
- Branch: `v8.0.0-bzl`
- Current HEAD at inspection time: `d200bfd77 chore: fix java/lance-jni opendal and hdrs version.`
- Relevant HDFS commits:
  - `6d2ac550a feat: support hdfs object store`
  - `e32fe8fa9 fix: import opendal send future trait`
  - `cabd48883 chore:test`

Files that define the reference behavior:

- `rust/lance-io/src/object_store/providers/hdfs.rs`
- `rust/lance-io/tests/hdfs_integration.rs`
- `rust/lance-io/Cargo.toml`, `rust/lance-table/Cargo.toml`,
  `rust/lance/Cargo.toml`
- `rust/lance-table/src/io/commit.rs`

The reference implementation uses:

- Apache OpenDAL `services-hdfs`
- `object_store_opendal::OpendalStore`
- Lance `ObjectStoreProvider`
- `lance_core::error::{Error, Result}`

### 2.2 Verified Lance Main APIs

Verified against upstream commit `2602724cf6256ff8f55571805fdc5b0614d706b8`
(2026-09-17, workspace version `13.0.0-beta.4`):

- PR #9123 upgraded Lance to `object_store 0.14.1`.
- Current main uses OpenDAL 0.59.2 and `object_store_opendal 0.60.2`.
- All direct Lance dependencies use the same official Git source and `main`
  branch. `Cargo.lock` pins the resolved commit; CI uses `--locked`.
- The development toolchain matches upstream Rust 1.97.0.
- HDFS is supplied by this external provider.
- `lance_io::object_store::ObjectStoreProvider` is public.
- `lance_io::object_store::ObjectStoreRegistry::insert` is public and takes
  `&self`, `scheme: &str`, and `Arc<dyn ObjectStoreProvider>`.
- `lance_io::object_store::ObjectStore::new` is public. This is the constructor
  that external crates must use to return Lance's `ObjectStore` type from a
  provider implementation.
- `lance-table` on the targeted main commit exposes
  `lance_table::io::commit::RenameCommitHandler`.

This makes a third-party provider possible without modifying the Lance source
tree.

### 2.3 Important Commit-Handler Boundary

The local `v8.0.0-bzl` branch has:

```rust
#[cfg(feature = "hdfs")]
"hdfs" => Ok(Arc::new(RenameCommitHandler)),
```

in `commit_handler_from_url`.

The targeted upstream `lance-table` source does **not** have this `hdfs` arm. Its
unknown-scheme fallback is `UnsafeCommitHandler`.

Therefore, registering the HDFS object store provider alone does **not** make
Lance automatically select `RenameCommitHandler` for HDFS dataset writes. The
third-party crate must document this and should expose a helper for the caller
to opt into the safe rename commit handler.

## 3. Scope

### 3.1 In Scope

- A single Rust library crate named `lance-hdfs-backend`.
- A `HdfsStoreProvider` that implements Lance's `ObjectStoreProvider`.
- A small registration helper for `ObjectStoreRegistry`.
- A `HdfsObjectStore` wrapper that adapts OpenDAL's HDFS operator to
  `object_store` semantics.
- Configuration resolution matching the local reference:
  - storage options
  - environment variables
  - HDFS URI authority
- Unit tests that do not require a live HDFS cluster.
- Ignored integration tests that can run against a real HDFS cluster.
- GitHub Actions CI for formatting, clippy, build, unit tests, package checks,
  HDFS integration, and dependency/package-file checks.
- Preserve crates.io metadata for a future release using registry dependencies.
  Git-only Lance dependencies currently prevent crates.io publishing.

### 3.2 Out Of Scope For The First Version

- Modifying upstream Lance or publishing a patched Lance release.
- Automatic global process-wide provider registration. Lance's default
  registry is not exposed as a public mutable global from `lance-io`; callers
  should create a `Session` with a registry that this crate has populated.
- `opendal` `services-hdfs-native` support. The local reference uses
  `services-hdfs`, so the first implementation should use that path.
- Python, Java, or Lance-JNI bindings.
- Multi-version Lance compatibility matrix. Development targets the official
  Lance main commit recorded in `Cargo.lock`.

## 4. Crate API Proposal

The public surface should stay small. Exact names and signatures must be
compiled and verified during implementation; the following is the intended
API:

```rust
pub const HDFS_SCHEME: &str = "hdfs";

/// Registers the HDFS provider in a Lance registry.
pub fn register(registry: &ObjectStoreRegistry);

/// HDFS object store provider backed by OpenDAL.
#[derive(Default, Debug)]
pub struct HdfsStoreProvider;

/// Optional safe-commit helper.
#[cfg(feature = "commit-handler")]
pub fn rename_commit_handler() -> Arc<dyn lance_table::io::commit::CommitHandler>;
```

`HdfsStoreProvider` should also remain directly usable:

```rust
registry.insert(
    "hdfs",
    Arc::new(HdfsStoreProvider),
);
```

The registration helper should call `ObjectStoreRegistry::insert` with
`Arc<HdfsStoreProvider>`.

## 5. Provider Behavior

### 5.1 URI and Path Semantics

- URI must have a host: `hdfs://namenode:9000/path`.
- `extract_path` should parse the URI path with
  `object_store::path::Path::parse`.
- Expected examples from the local reference:
  - `hdfs://namenode:9000/path/to/file` -> `path/to/file`
  - `hdfs://namenode:9000/` -> empty path
  - `hdfs://ht-hdfsqa/user/data/file.txt` -> `user/data/file.txt`

### 5.2 Configuration Mapping

| Lance storage option | Environment variable | OpenDAL HDFS option |
| --- | --- | --- |
| `hdfs_name_node` | `HDFS_NAME_NODE` | `name_node` |
| `hdfs_user` | `HADOOP_USER_NAME`, then `HDFS_USER` | `user` |
| `hdfs_kerberos_ticket_cache_path` | none | `kerberos_ticket_cache_path` |
| `hdfs_atomic_write_dir` | none | `atomic_write_dir` |

Resolution order for `name_node`:

1. `storage_options["hdfs_name_node"]`, when non-empty
2. `HDFS_NAME_NODE`, when non-empty
3. `hdfs://` plus the URI authority

Resolution order for user:

1. `storage_options["hdfs_user"]`, when non-empty
2. `HADOOP_USER_NAME`, when non-empty
3. `HDFS_USER`, when non-empty

Fixed configuration in the reference implementation:

```text
root=/            rename_overwrite=false
```

### 5.3 OpenDAL Operator

Use:

```rust
Operator::from_iter::<Hdfs>(config)
    .map_err(...)?
```

where `Hdfs` is `opendal::services::Hdfs`.

Operator creation errors should preserve connection context:

- the effective `name_node`
- whether a user was configured

The local reference formats this as:

```text
Failed to create HDFS operator: {error}. name_node={name_node}, has_user={has_user}
```

### 5.4 Object Store Wrapper

Port the local `HdfsObjectStore`:

- Store both `OpendalStore` and the original `Operator`.
- Delegate normal operations to `OpendalStore` using shared `object_store 0.14.1`
  types, preserving options, results, attributes, extensions, and errors.
- Do not add an `object_store_014` alias or cross-version conversion layer.
  DataFusion may still pull in 0.13 transitively; Lance owns that boundary.
- For `rename_opts` with `RenameTargetMode::Create`, call OpenDAL
  `rename` directly after percent-decoding both paths.
- For other rename target modes, delegate to `OpendalStore`.
- Map OpenDAL errors to `object_store::Error` in the same way as the local
  reference:
  - `NotFound`
  - `AlreadyExists`
  - `Unsupported` -> `NotSupported`
  - `ConditionNotMatch` -> `Precondition`
  - otherwise `Generic`

### 5.5 Store Prefix

`calculate_object_store_prefix` must reflect the effective NameNode, not only
the URI authority:

1. `hdfs_name_node` storage option
2. `HDFS_NAME_NODE`
3. URI authority

The local reference format is:

```text
hdfs${effective_authority_or_name_node}
```

Preserve that behavior in the first port.

## 6. Cargo.toml Proposal

The manifest tracks official Lance main with OpenDAL 0.59.2. Validate dependency
resolution with `cargo metadata`; compile and test through remote CI. Local
Cargo compilation and test execution are disabled by the working agreement.

```toml
[package]
name = "lance-hdfs-backend"
version = "0.1.0"
edition = "2024"
rust-version = "1.91.0"
authors = ["<replace with owner>"]
description = "HDFS object store backend for Lance, built on Apache OpenDAL"
license = "Apache-2.0"
repository = "https://github.com/hfutatzhanghb/lance-hdfs-backend"
documentation = "https://docs.rs/lance-hdfs-backend"
readme = "README.md"
keywords = ["lance", "hdfs", "object-store", "storage"]
categories = ["database-implementations", "filesystem"]

[dependencies]
async-trait = "0.1"
bytes = "1.11.1"
futures = "0.3"
object_store = { version = "=0.14.1", default-features = false }
url = "2.5.7"
lance-core = { git = "https://github.com/lance-format/lance.git", branch = "main", default-features = false }
lance-io = { git = "https://github.com/lance-format/lance.git", branch = "main", default-features = false }
opendal = { version = "0.59.2", optional = true }
object_store_opendal = { version = "0.59.2", optional = true }
lance-table = { git = "https://github.com/lance-format/lance.git", branch = "main", optional = true, default-features = false }

[dev-dependencies]
arrow-array = "58.0.0"
arrow-schema = "58.0.0"
lance = { git = "https://github.com/lance-format/lance.git", branch = "main", default-features = false }
opendal = { version = "0.59.2", default-features = false, features = ["services-memory"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }

[features]
default = ["hdfs", "commit-handler"]
hdfs = [
    "dep:opendal",
    "opendal/services-hdfs",
    "dep:object_store_opendal",
]
commit-handler = ["dep:lance-table"]
```

Notes:

- Keep `lance-core`, `lance-io`, optional `lance-table`, and test dependency
  `lance` on the same Git source/branch and lockfile commit to avoid type drift.
- `lance-io` should use `default-features = false` because the HDFS provider
  does not need Lance's AWS/Azure/GCP defaults.
- Keep `opendal` default features enabled; the `services-hdfs` feature is added
  on top of them.
- `license`, `description`, and `repository` are required or strongly
  recommended for the crates.io release.
- `documentation` is set to the eventual docs.rs URL.

## 7. Repository Layout Proposal

```text
lance-hdfs-backend/
  .github/
    workflows/
      ci.yml
      hdfs-integration.yml
      publish.yml
  src/
    lib.rs
    provider.rs
    config.rs
    hdfs_object_store.rs
  tests/
    config.rs
    hdfs_integration.rs
  examples/
    lance_dataset.rs
  Cargo.toml
  Cargo.lock
  README.md
  LICENSE
  NOTICE
```

The exact module split is an implementation detail; the important boundary is
to keep config/path resolution testable without constructing a live HDFS
operator.

## 8. Test Plan

### 8.1 Unit Tests Without HDFS

Port and adapt these reference tests:

- URI path extraction
- URL-only configuration
- storage-option override precedence
- environment-variable override precedence
- missing host rejection
- operator error context

These tests should pass with only Java available for build-time `hdrs`/JNI
setup and should not require an HDFS daemon.

### 8.2 Integration Tests

Default integration tests should be `#[ignore]`, as in the local reference.
They should cover:

- store creation
- custom storage options
- basic put/get/delete
- HA-style nameservice URL parsing

When run manually or by the integration workflow, set:

```text
HDFS_NAME_NODE=hdfs://<namenode>:<port>
```

### 8.3 Lance Dataset Test

Add a manual or integration example that:

1. Creates a Lance `Session`.
2. Registers `HdfsStoreProvider`.
3. Writes a dataset to an HDFS URI with `RenameCommitHandler`.
4. Opens and scans the dataset.

This test should be behind an ignored integration test or example until HDFS is
available in CI.

## 9. GitHub Actions Design

### 9.1 References Used

The following workflows were inspected, not invented:

- `lance-format/lance`:
  - `.github/workflows/rust.yml`
  - `.github/workflows/cargo-publish.yml`
- `lancedb/lance-spark`:
  - `.github/workflows/spark.yml`
  - `.github/workflows/publish.yml`
- Supporting HDFS build/runtime reference:
  - `Xuanwo/hdrs/.github/workflows/ci.yml`

`lance-spark` is Java/Scala focused, so borrow its workflow structure and
release trigger/concurrency/matrix patterns, not its Maven commands.

### 9.2 Main CI: `.github/workflows/ci.yml`

Trigger:

- `push` to `main`
- `pull_request` targeting `main`

Jobs:

1. **Format**
   - Setup Rust with `rustfmt`.
   - `cargo fmt --all -- --check`

2. **Clippy**
   - Setup Java before Cargo compilation because `hdrs`/`hdfs-sys` need JVM
     headers and native library resolution.
   - `cargo clippy --locked --all-targets --all-features -- -D warnings`

3. **Build and test**
   - Setup Java.
   - `cargo test --locked --all-features --no-fail-fast` compiles and runs tests.

4. **Dependencies and package contents**
   - `cargo metadata --locked --format-version 1 --all-features`
   - `cargo package --list --locked --all-features`
   - Git-only Lance main dependencies cannot be packaged for crates.io.
     Restore the full package check when targeting a registry release.

5. **Rustdoc**
   - `RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features`

Common settings borrowed from the inspected workflows:

- `permissions: contents: read`
- `concurrency` with cancel-in-progress
- `CARGO_TERM_COLOR: always`
- `RUST_BACKTRACE: "1"`
- cache where practical (`Swatinem/rust-cache` or equivalent)

Java setup recommendation:

```yaml
- uses: actions/setup-java@v6
  with:
    distribution: temurin
    java-version: "17"
```

### 9.3 HDFS Integration: `.github/workflows/hdfs-integration.yml`

This job runs on pushes to `main`, `workflow_dispatch`, and a weekly schedule.
It is not triggered for pull requests. It should:

1. Set up Rust and Java.
2. Install a matching Hadoop client distribution.
3. Set `HADOOP_HOME`.
4. Export a Hadoop-generated `CLASSPATH`.
5. Set `LD_LIBRARY_PATH` to include `$JAVA_HOME/lib/server` and, when using a
   dynamic `libhdfs`, `$HADOOP_HOME/lib/native`.
6. Start a namenode/datanode pair, ideally with Docker and host networking.
7. Wait for the namenode HTTP endpoint.
8. Run the ignored HDFS integration tests:

```bash
cargo test --locked --all-features --test hdfs_integration -- --ignored
```

Use the HDFS container/runtime setup from `Xuanwo/hdrs/.github/workflows/ci.yml`
as the starting reference. The Hadoop client and cluster version should match
the image version actually chosen.

### 9.4 Publish: `.github/workflows/publish.yml`

This workflow applies to future release tags with registry-based Lance
dependencies. The current Git-only main development branch cannot be published
to crates.io; switch to a compatible registry release before using it.

Trigger:

- GitHub release marked `released`
- or `workflow_dispatch` with a tag input

Steps:

1. Check out the tag.
2. Setup Rust and Java.
3. Run:

```bash
cargo publish --locked --all-features
```

4. Use `CARGO_REGISTRY_TOKEN` from GitHub Actions secrets.

Also require `cargo publish --dry-run` and `cargo package` to pass in CI before
the actual publish.

## 10. Release and Publish Checklist

- [ ] Replace Git-only Lance dependencies with a compatible crates.io release
- [ ] Repository exists at `https://github.com/hfutatzhanghb/lance-hdfs-backend`
- [ ] `Cargo.toml` has `license = "Apache-2.0"`
- [ ] `Cargo.toml` has a non-empty `description`
- [ ] `Cargo.toml` has a valid `repository`
- [ ] `Cargo.toml` has `documentation`
- [ ] `README.md` exists
- [ ] `LICENSE` exists
- [ ] `NOTICE` documents Lance-derived code when needed
- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes
- [ ] `cargo test --all-features` passes
- [ ] `cargo package --no-verify --all-features` passes
- [ ] `cargo publish --dry-run` passes
- [ ] Optional HDFS integration workflow passes against a real cluster

## 11. Open Decisions And Assumptions

These should be confirmed before implementation:

1. **GitHub owner**: The authenticated `gh` account observed during inspection
   is `hfutatzhanghb`; `hfutatzhanghb/lance-hdfs-backend` returned 404, so the
   repository does not exist yet. This spec assumes that account and repo name.
2. **License**: `Apache-2.0` is assumed because the source reference and Lance
   are Apache-2.0. If the owner prefers MIT, the source attribution and notice
   strategy should be reviewed.
3. **Initial version**: `0.1.0` is assumed.
4. **Commit-handler feature**: The draft makes `commit-handler` a default
   feature. It can be made non-default if keeping the dependency graph smaller
   matters more than the convenient safe-commit helper.
5. **HDFS client version**: The reference CI source uses Hadoop 3.1.3 for its
   cluster job and 3.3.5 for another job. The final integration workflow must
   pin one consistent version and verify it against the chosen Docker images.

## 12. First Implementation Milestones

1. Confirm owner, create repository, rename local default branch to `main`.
2. Add license, notice, readme, Cargo manifest, and `.gitignore`.
3. Port config resolution and provider code into the standalone crate.
4. Port unit tests and make `cargo test` pass without an HDFS cluster.
5. Add ignored integration tests and examples.
6. Add CI, package check, optional HDFS integration, and publish workflow.
7. Run `cargo package`/`cargo publish --dry-run`.
8. Create the first GitHub release and publish from the workflow.
