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

Development follows the official Lance `main` branch.

## Requirements

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

## Usage

Register the provider, create a session, and open a dataset:

```rust,ignore
use std::sync::Arc;

use lance::dataset::{
    DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE, builder::DatasetBuilder,
};
use lance::session::Session;
use lance_hdfs_backend::{register, rename_commit_handler};
use lance_io::object_store::ObjectStoreRegistry;

let registry = Arc::new(ObjectStoreRegistry::default());
register(&registry);

let session = Arc::new(Session::new(
    DEFAULT_INDEX_CACHE_SIZE,
    DEFAULT_METADATA_CACHE_SIZE,
    registry,
));

let dataset = DatasetBuilder::from_uri("hdfs://namenode:9000/user/data/dataset")
    .with_session(session.clone())
    .with_commit_handler(rename_commit_handler())
    .load()
    .await?;
```

For writes, set both `session` and `commit_handler`:

```rust,ignore
use lance::dataset::{WriteMode, WriteParams};
use lance_hdfs_backend::rename_commit_handler;

let params = WriteParams {
    mode: WriteMode::Overwrite,
    session: Some(session),
    commit_handler: Some(rename_commit_handler()),
    ..Default::default()
};
```

Pass `rename_commit_handler()` explicitly when writing datasets to enable
atomic HDFS commits. Registering the storage provider alone does not configure
the commit handler.

See [the dataset example](examples/lance_dataset.rs) for a complete write and
read workflow.

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

## Integration Tests

The HDFS integration tests are ignored by default:

```bash
HDFS_NAME_NODE=hdfs://localhost:9000 \
  cargo test --all-features --test hdfs_integration -- --ignored
```

## License

Licensed under the Apache License, Version 2.0.
