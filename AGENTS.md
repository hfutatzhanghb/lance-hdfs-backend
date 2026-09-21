# AGENTS.md

lance-hdfs-backend is a Rust crate that lets Lance read and write datasets on
HDFS. It implements lance-io's `ObjectStoreProvider` for the `hdfs://` scheme
on top of Apache OpenDAL, and provides a rename-based commit handler that uses
HDFS atomic renames to publish dataset versions safely.

## Development Commands

* Format check: `cargo fmt --all -- --check`
* Format: `cargo fmt --all`
* Lint: `cargo clippy --locked --all-targets --all-features -- -D warnings`
* Test: `cargo test --locked --all-features --no-fail-fast`
* Docs: `RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features`
* Package check: `cargo package --list --locked --all-features`
* MSRV check (Rust 1.91.0): `RUSTUP_TOOLCHAIN=1.91.0 cargo check --locked --all-targets --all-features`
* HDFS integration tests are ignored by default and need a live cluster:
  `HDFS_NAME_NODE=hdfs://localhost:9000 cargo test --all-features --test hdfs_integration -- --ignored`

Building requires a Java 17 JDK and `protoc`, matching the CI workflow. At
test time, `libjvm` must be reachable, e.g. `LD_LIBRARY_PATH=$JAVA_HOME/lib/server`.

## Coding Standards

- Always use English in code, examples, and comments.
- Comments should explain non-obvious "why" reasoning, not restate what the
  code does.
- Remove debug prints (`println!`, `dbg!`) before merging; use `log` or
  `tracing` where output is genuinely needed.
- All bugfixes and features must have corresponding tests.

## Dependency Compatibility

- Lance crates use caret requirements (`"12"`), never exact pins: `lance-core`,
  `lance-io`, `lance-table`, and the `lance` dev-dependency. An exact `=12.0.0`
  turns the next `lance` patch release into an unresolvable dependency graph for
  users, while a caret still resolves every crate to one `lance-io` copy.
- One published `lance` major is one backend release line. Do not mix majors:
  `lance-io` 12 and 13 are semver-incompatible, so cargo keeps two copies and
  the provider no longer type-checks against the registry.
- The storage stack must stay type-compatible with Lance `v12.0.0`:
  `lance-io 12` <-> `object_store 0.14.1` <-> `object_store_opendal 0.60.1`
  <-> `opendal 0.59.1`. Never bump one of these without checking the others;
  blind auto-upgrades break the build at the type level.
- Published releases must depend only on crates.io versions, never Git
  dependencies; crates.io rejects them.

## Commit Messages

- Use Conventional Commits for every commit: `feat:`, `fix:`, `docs:`,
  `perf:`, `ci:`, `test:`, `build:`, `style:`, or `chore:`, with an optional
  scope such as `feat(hdfs):`.
- Mark breaking changes with `!` before the colon (`feat!: ...`) and/or a
  `BREAKING CHANGE:` footer.

## Pull Requests

- Before creating a PR, search for similar open PRs and inspect any PRs linked
  to the issue being addressed, to avoid duplicate work.
- PR titles must follow the Conventional Commits specification because
  `.github/workflows/pr-title.yml` validates the PR title and body with
  commitlint. Use prefixes like `feat:`, `fix:`, `docs:`, `perf:`, `ci:`,
  `test:`, `build:`, `style:`, or `chore:`; add a scope when useful. The PR
  title and description are used as the merge commit message.
- The same workflow's `Label PR` job applies labels from
  `.github/labeler.yml` based on the conventional type in the title or body
  (`breaking-change`, `enhancement`, `bug`, `documentation`, `performance`,
  `ci`, `chore`). Those labels must already exist in the repository.
- Before creating or updating a PR, run `cargo fmt --all` and
  `cargo clippy --locked --all-targets --all-features -- -D warnings`. If a
  required check cannot be run locally, state the blocker explicitly in the
  PR summary; the `CI` and `HDFS integration` workflows are the final
  verifiers.
- Keep PRs focused: no drive-by refactors, reformatting, or cosmetic changes.

## Filing Issues

- When opening an issue with `gh issue create` or the API, pass an explicit
  label (`--label bug`, `--label enhancement`, `--label documentation`, ...);
  nothing applies issue labels automatically in this repository.
- Prefix the title to match the type, e.g. `bug: ...`, `feat: ...`, `docs: ...`.

## Review Guidelines

- Be concise and focus on P0/P1 issues: severe bugs, performance regressions,
  security concerns.
- Check naming consistency, error handling patterns, and test coverage.
