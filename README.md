# lance-hdfs-backend

HDFS object store backend for [Lance](https://github.com/lance-format/lance),
built on Apache OpenDAL's `services-hdfs`.

## Requirements

- Rust 1.91 or newer
- Java 11 or newer
- Hadoop client libraries and configuration when connecting to HDFS

OpenDAL's HDFS service uses `hdrs` and `hdfs-sys`. Set the following for builds
and runtime:

```text
JAVA_HOME
HADOOP_HOME
HADOOP_CONF_DIR
CLASSPATH
LD_LIBRARY_PATH
```

When no prebuilt `libhdfs` is found, `hdfs-sys` falls back to compiling its
bundled native client from source.

## Usage

```rust,ignore
use std::sync::Arc;

use lance::dataset::{
    DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE, WriteParams, builder::DatasetBuilder,
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

`register` only registers the object store provider. Lance 8.0.0 does not know
the `hdfs` scheme when selecting a commit handler, so writers must pass the
returned `RenameCommitHandler` explicitly. Failing to do so can fall back to
`UnsafeCommitHandler` and is unsafe with concurrent writers.

## Configuration

| Lance storage option | Environment variable | OpenDAL HDFS option |
| --- | --- | --- |
| `hdfs_name_node` | `HDFS_NAME_NODE` | `name_node` |
| `hdfs_user` | `HADOOP_USER_NAME`, then `HDFS_USER` | `user` |
| `hdfs_kerberos_ticket_cache_path` | none | `kerberos_ticket_cache_path` |
| `hdfs_atomic_write_dir` | none | `atomic_write_dir` |

Configuration priority is storage options, environment variables, then the URI
authority.

## Integration Tests

The HDFS integration tests are ignored by default:

```bash
HDFS_NAME_NODE=hdfs://localhost:9000 \
  cargo test --all-features --test hdfs_integration -- --ignored
```

## License

Licensed under the Apache License, Version 2.0.
