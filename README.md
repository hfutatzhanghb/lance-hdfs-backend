# lance-hdfs-backend

Read and write [Lance](https://github.com/lance-format/lance) datasets on HDFS
with a pluggable Rust storage backend, powered by Apache OpenDAL.

## Features

- **Lance datasets on HDFS** — create, read, and append to datasets using
  `hdfs://` URIs, with access to committed dataset versions.
- **Atomic dataset commits** — use HDFS atomic renames to publish new versions
  without overwriting an existing version during concurrent writes.
- **Session-based integration** — register the HDFS provider in a Lance storage
  registry and share it through a session for reads and writes.
- **Storage operations** — stream file contents, read byte ranges, write data,
  list objects, and delete files through Lance's storage interface.
- **Hadoop configuration** — configure the NameNode, HDFS user, Kerberos ticket
  cache, and atomic write directory through storage options and supported
  environment variables.

## Installation

Add the backend and the Lance crates it integrates with to your `Cargo.toml`:

```toml
[dependencies]
lance-hdfs-backend = "0.1.2"
lance = { version = "12.0.0", default-features = false }
lance-io = { version = "12.0.0", default-features = false }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

`lance-io` provides the storage registry the provider is registered in, `lance`
provides the dataset API, and `tokio` runs the async calls. The default features
of `lance-hdfs-backend` include the HDFS provider and the rename commit handler.

This release targets Lance 12.0.0. Keep `lance` and `lance-io` on that version
so the backend and your application share the same types.

## Quickstart

### Prepare the environment

Before connecting to a cluster, make sure you have:

- The Rust toolchain specified in [rust-toolchain.toml](rust-toolchain.toml)
- Java 11 or newer
- Hadoop client libraries and configuration when connecting to HDFS

Configure the Java and Hadoop environment for your cluster:

```text
JAVA_HOME
HADOOP_HOME
HADOOP_CONF_DIR
CLASSPATH
LD_LIBRARY_PATH
```

Register the provider, create a session, and read an existing HDFS dataset.
Replace the URI with your NameNode and dataset path:

```rust,ignore
use std::sync::Arc;

use lance::dataset::{
    DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE, builder::DatasetBuilder,
};
use lance::session::Session;
use lance_hdfs_backend::{register, rename_commit_handler};
use lance_io::object_store::ObjectStoreRegistry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = Arc::new(ObjectStoreRegistry::default());
    register(&registry);

    let session = Arc::new(Session::new(
        DEFAULT_INDEX_CACHE_SIZE,
        DEFAULT_METADATA_CACHE_SIZE,
        registry,
    ));

    let dataset = DatasetBuilder::from_uri("hdfs://namenode:9000/user/data/dataset")
        .with_session(session)
        .with_commit_handler(rename_commit_handler())
        .load()
        .await?;

    println!("rows={}", dataset.count_rows(None).await?);
    Ok(())
}
```

To create or append to a dataset, pass the same session and
`rename_commit_handler()` through `WriteParams`, and select `WriteMode::Create`
or `WriteMode::Append`. See [the dataset example](examples/lance_dataset.rs)
for a complete write and read workflow, including Arrow batch construction.

## Configuration

| Storage option | Environment variable | Purpose |
| --- | --- | --- |
| `hdfs_name_node` | `HDFS_NAME_NODE` | NameNode address or nameservice URI |
| `hdfs_user` | `HADOOP_USER_NAME`, then `HDFS_USER` | HDFS user identity |
| `hdfs_kerberos_ticket_cache_path` | none | Kerberos ticket cache path |
| `hdfs_atomic_write_dir` | none | Temporary directory for atomic writes |

For the NameNode, storage options take precedence over the environment, then
the address in the `hdfs://` URI. An explicit HDFS user takes precedence over
the user environment variables.

## Notes

- **HDFS URIs:** include a NameNode address, such as
  `hdfs://namenode:8020/user/data/dataset`, or a nameservice configured in your
  Hadoop client, such as `hdfs://my-cluster/user/data/dataset`.
- **Dataset commits:** pass `rename_commit_handler()` explicitly for writes.
  It uses HDFS atomic renames to avoid overwriting an existing dataset version.
  Registering the storage provider alone does not configure the commit handler.
- **Authentication:** use the storage options listed above for user identity
  and a Kerberos ticket cache. Keep the Hadoop client configuration consistent
  with the target cluster.
- **Configuration scope:** the provider forwards the listed HDFS options and
  sets the storage root to `/`. Create-mode renames (dataset commits) request
  OpenDAL if-not-exists semantics, so a commit fails instead of replacing an
  already committed dataset version.

## Integration Tests

The HDFS integration tests are ignored by default:

```bash
HDFS_NAME_NODE=hdfs://localhost:9000 \
  cargo test --all-features --test hdfs_integration -- --ignored
```

## Licenses

Licensed under the Apache License, Version 2.0. See the `LICENSE` file for the
license text.

This project includes code originally developed by the Lance Authors.
See the `NOTICE` file for attribution.
