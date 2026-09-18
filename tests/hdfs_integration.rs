// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Integration tests for the HDFS object store provider.
//!
//! These tests require a live HDFS cluster and are ignored by default.

#[cfg(feature = "hdfs")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use futures::TryStreamExt;
    use lance_hdfs_backend::register;
    use lance_io::object_store::{
        ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
    };
    use object_store::path::Path;
    use object_store::{
        ObjectMeta, ObjectStoreExt, PutMode, PutOptions, RenameOptions, RenameTargetMode,
    };

    fn registry() -> Arc<ObjectStoreRegistry> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        register(&registry);
        registry
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    fn create_options() -> RenameOptions {
        RenameOptions {
            target_mode: RenameTargetMode::Create,
            ..Default::default()
        }
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_store_creation() {
        let url = "hdfs://localhost:9000/test/path";
        let (store, path) =
            ObjectStore::from_uri_and_params(registry(), url, &ObjectStoreParams::default())
                .await
                .unwrap();

        assert_eq!(store.scheme(), "hdfs");
        assert_eq!(path, Path::from("test/path"));
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_store_with_custom_config() {
        let url = "hdfs://namenode:9000/user/test";
        let storage_options = HashMap::from([
            ("hdfs_user".to_string(), "testuser".to_string()),
            (
                "hdfs_name_node".to_string(),
                "hdfs://localhost:9000".to_string(),
            ),
        ]);
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                storage_options,
            ))),
            ..Default::default()
        };

        let (store, path) = ObjectStore::from_uri_and_params(registry(), url, &params)
            .await
            .unwrap();

        assert_eq!(store.scheme(), "hdfs");
        assert_eq!(path, Path::from("user/test"));
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_basic_operations() {
        let (store, _) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://localhost:9000/test",
            &Default::default(),
        )
        .await
        .unwrap();
        let test_path = Path::from("test_file.txt");
        let test_data = bytes::Bytes::from("Hello, HDFS!");

        store
            .inner
            .put(&test_path, test_data.clone().into())
            .await
            .unwrap();
        let read_data = store
            .inner
            .get(&test_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(read_data, test_data);
        store.inner.delete(&test_path).await.unwrap();
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_ha_configuration() {
        // Manual entry point for nameservice (HA) clusters: export
        // HDFS_HA_NAMESERVICE and provide a Hadoop client configuration that
        // resolves the nameservice. Automated CI runs a single-namenode
        // cluster, so this test skips itself there.
        let Some(nameservice) = std::env::var("HDFS_HA_NAMESERVICE")
            .ok()
            .filter(|value| !value.is_empty())
        else {
            eprintln!("HDFS_HA_NAMESERVICE not set; skipping HA configuration test");
            return;
        };

        let url = format!("hdfs://{nameservice}/user/test");
        let (store, path) = ObjectStore::from_uri_and_params(registry(), &url, &Default::default())
            .await
            .unwrap();

        assert_eq!(store.scheme(), "hdfs");
        assert_eq!(path, Path::from("user/test"));
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_rename_create_mode_rejects_existing_target() {
        let (store, _) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://localhost:9000/test",
            &Default::default(),
        )
        .await
        .unwrap();

        let suffix = unique_suffix();
        let winner = Path::from(format!("rename-winner-{suffix}.manifest"));
        let loser = Path::from(format!("rename-loser-{suffix}.manifest"));
        store
            .inner
            .put(&winner, bytes::Bytes::from_static(b"v2-a").into())
            .await
            .unwrap();
        store
            .inner
            .put(&loser, bytes::Bytes::from_static(b"v2-b").into())
            .await
            .unwrap();

        // Simulates the losing side of a concurrent dataset commit: a
        // create-mode rename onto an already committed version must fail and
        // must never replace the committed file.
        let error = store
            .inner
            .rename_opts(&loser, &winner, create_options())
            .await
            .unwrap_err();
        assert!(matches!(error, object_store::Error::AlreadyExists { .. }));

        assert_eq!(
            store
                .inner
                .get(&winner)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes::Bytes::from_static(b"v2-a")
        );
        assert!(store.inner.head(&loser).await.is_ok());

        // Create-mode rename onto a free target still succeeds and removes
        // the source.
        let free = Path::from(format!("rename-free-{suffix}.manifest"));
        store
            .inner
            .rename_opts(&loser, &free, create_options())
            .await
            .unwrap();
        assert!(matches!(
            store.inner.get(&loser).await,
            Err(object_store::Error::NotFound { .. })
        ));

        store.inner.delete(&winner).await.unwrap();
        store.inner.delete(&free).await.unwrap();
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_error_mapping() {
        let (store, _) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://localhost:9000/test",
            &Default::default(),
        )
        .await
        .unwrap();

        let suffix = unique_suffix();
        let missing = Path::from(format!("missing-{suffix}.bin"));
        assert!(matches!(
            store.inner.get(&missing).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            store.inner.head(&missing).await,
            Err(object_store::Error::NotFound { .. })
        ));

        // The OpenDAL HDFS service exposes no write-if-not-exists capability,
        // so the OpenDAL correctness layer rejects create-mode puts as
        // Unsupported. The invariant that matters for commits is that a
        // create-mode put never silently overwrites, so accept any error.
        let path = Path::from(format!("create-put-{suffix}.bin"));
        store
            .inner
            .put(&path, Bytes::from_static(b"v1").into())
            .await
            .unwrap();
        let result = store
            .inner
            .put_opts(
                &path,
                Bytes::from_static(b"v2").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        assert!(
            result.is_err(),
            "create-mode put must not silently overwrite"
        );
        assert_eq!(
            store.inner.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"v1")
        );
        store.inner.delete(&path).await.unwrap();
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_get_ranges() {
        let (store, _) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://localhost:9000/test",
            &Default::default(),
        )
        .await
        .unwrap();

        let path = Path::from(format!("ranges-{}.bin", unique_suffix()));
        store
            .inner
            .put(&path, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();

        let ranges = store
            .inner
            .get_ranges(&path, &[0..4, 6..10, 15..16])
            .await
            .unwrap();
        assert_eq!(
            ranges,
            vec![
                Bytes::from_static(b"0123"),
                Bytes::from_static(b"6789"),
                Bytes::from_static(b"f"),
            ]
        );

        store.inner.delete(&path).await.unwrap();
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_list_operations() {
        let (store, _) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://localhost:9000/test",
            &Default::default(),
        )
        .await
        .unwrap();

        let root = format!("list-{}", unique_suffix());
        let files = [
            format!("{root}/a.txt"),
            format!("{root}/b.txt"),
            format!("{root}/nested/c.txt"),
        ];
        for file in &files {
            store
                .inner
                .put(&Path::from(file.as_str()), Bytes::from_static(b"x").into())
                .await
                .unwrap();
        }

        let prefix = Path::from(format!("{root}/"));

        let all: Vec<ObjectMeta> = store.inner.list(Some(&prefix)).try_collect().await.unwrap();
        assert_eq!(all.len(), 3);

        let delimited = store
            .inner
            .list_with_delimiter(Some(&prefix))
            .await
            .unwrap();
        assert_eq!(delimited.objects.len(), 2);
        assert_eq!(
            delimited.common_prefixes,
            vec![Path::from(format!("{root}/nested/"))]
        );

        let after_a: Vec<ObjectMeta> = store
            .inner
            .list_with_offset(Some(&prefix), &Path::from(format!("{root}/a.txt")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(after_a.len(), 2);

        for file in &files {
            store
                .inner
                .delete(&Path::from(file.as_str()))
                .await
                .unwrap();
        }
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_copy_is_reported_as_unsupported() {
        // The OpenDAL HDFS service exposes no copy capability. Copies must
        // surface as NotSupported instead of silently falling back to a
        // non-atomic read/write path.
        let (store, _) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://localhost:9000/test",
            &Default::default(),
        )
        .await
        .unwrap();

        let suffix = unique_suffix();
        let src = Path::from(format!("copy-src-{suffix}.bin"));
        let dst = Path::from(format!("copy-dst-{suffix}.bin"));
        store
            .inner
            .put(&src, Bytes::from_static(b"payload").into())
            .await
            .unwrap();

        let result = store.inner.copy(&src, &dst).await;
        assert!(matches!(
            result,
            Err(object_store::Error::NotSupported { .. })
        ));

        store.inner.delete(&src).await.unwrap();
    }

    fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            out.extend_from_slice(&state.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_hdfs_multipart_large_file_roundtrip() {
        let (store, _) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://localhost:9000/test",
            &Default::default(),
        )
        .await
        .unwrap();

        const PART_LEN: usize = 5 * 1024 * 1024;
        const PARTS: usize = 8;
        let path = Path::from(format!("multipart-{}.bin", unique_suffix()));
        let data = pseudo_random_bytes(PART_LEN * PARTS, 42);

        let mut upload = store.inner.put_multipart(&path).await.unwrap();
        for part in data.chunks(PART_LEN) {
            upload
                .put_part(Bytes::copy_from_slice(part).into())
                .await
                .unwrap();
        }
        upload.complete().await.unwrap();

        let meta = store.inner.head(&path).await.unwrap();
        assert_eq!(meta.size, data.len() as u64);

        let total = data.len();
        let ranges = store
            .inner
            .get_ranges(
                &path,
                &[
                    PART_LEN as u64 - 3..PART_LEN as u64 + 3,
                    total as u64 - 7..total as u64,
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            ranges[0],
            Bytes::copy_from_slice(&data[PART_LEN - 3..PART_LEN + 3])
        );
        assert_eq!(ranges[1], Bytes::copy_from_slice(&data[total - 7..]));

        let full = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(full.as_ref(), data.as_slice());

        store.inner.delete(&path).await.unwrap();
    }
}

#[cfg(all(feature = "hdfs", feature = "commit-handler"))]
mod dataset_tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use lance::Dataset;
    use lance::dataset::{
        DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE, WriteMode, WriteParams,
        builder::DatasetBuilder,
    };
    use lance::session::Session;
    use lance_hdfs_backend::{register, rename_commit_handler};
    use lance_io::object_store::ObjectStoreRegistry;

    fn session() -> Arc<Session> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        register(&registry);
        Arc::new(Session::new(
            DEFAULT_INDEX_CACHE_SIZE,
            DEFAULT_METADATA_CACHE_SIZE,
            registry,
        ))
    }

    fn batches(schema: Arc<Schema>, values: Vec<u32>) -> impl RecordBatchReader + Send {
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt32Array::from(values))])
            .unwrap();
        RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema)
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_dataset_commits_use_rename_handler_and_select_latest_version() {
        let session = session();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let uri = format!("hdfs://localhost:9000/lance-hdfs-backend-test-{suffix}");

        Dataset::write(
            batches(schema.clone(), vec![1, 2, 3]),
            uri.as_str(),
            Some(WriteParams {
                mode: WriteMode::Overwrite,
                session: Some(session.clone()),
                commit_handler: Some(rename_commit_handler()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        Dataset::write(
            batches(schema.clone(), vec![4, 5, 6]),
            uri.as_str(),
            Some(WriteParams {
                mode: WriteMode::Append,
                session: Some(session.clone()),
                commit_handler: Some(rename_commit_handler()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let dataset = DatasetBuilder::from_uri(uri.as_str())
            .with_session(session.clone())
            .with_commit_handler(rename_commit_handler())
            .load()
            .await
            .unwrap();

        assert_eq!(dataset.latest_version_id().await.unwrap(), 2);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 6);
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test]
    async fn test_dataset_time_travel_reads_committed_versions() {
        let session = session();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let uri = format!("hdfs://localhost:9000/lance-hdfs-backend-timetravel-{suffix}");

        Dataset::write(
            batches(schema.clone(), vec![1, 2, 3]),
            uri.as_str(),
            Some(WriteParams {
                mode: WriteMode::Overwrite,
                session: Some(session.clone()),
                commit_handler: Some(rename_commit_handler()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        Dataset::write(
            batches(schema.clone(), vec![4, 5, 6]),
            uri.as_str(),
            Some(WriteParams {
                mode: WriteMode::Append,
                session: Some(session.clone()),
                commit_handler: Some(rename_commit_handler()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let dataset = DatasetBuilder::from_uri(uri.as_str())
            .with_session(session.clone())
            .with_commit_handler(rename_commit_handler())
            .load()
            .await
            .unwrap();
        assert_eq!(dataset.latest_version_id().await.unwrap(), 2);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 6);

        let first = dataset.checkout_version(1u64).await.unwrap();
        assert_eq!(first.count_rows(None).await.unwrap(), 3);
    }

    #[ignore = "Requires HDFS cluster"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dataset_concurrent_appends_do_not_lose_writes() {
        let session = session();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let uri = format!("hdfs://localhost:9000/lance-hdfs-backend-concurrent-{suffix}");

        Dataset::write(
            batches(schema.clone(), vec![1, 2, 3]),
            uri.as_str(),
            Some(WriteParams {
                mode: WriteMode::Overwrite,
                session: Some(session.clone()),
                commit_handler: Some(rename_commit_handler()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let append = |values: Vec<u32>| {
            Dataset::write(
                batches(schema.clone(), values),
                uri.as_str(),
                Some(WriteParams {
                    mode: WriteMode::Append,
                    session: Some(session.clone()),
                    commit_handler: Some(rename_commit_handler()),
                    ..Default::default()
                }),
            )
        };
        let (first, second) = tokio::join!(append(vec![4, 5, 6]), append(vec![7, 8, 9]));

        // lance 12 does not retry commit conflicts in Dataset::write (the
        // retry executor covers update/delete/merge-insert), so with correct
        // create-only rename semantics exactly one appender wins the race for
        // version 2. Assert the weaker consistency invariant so the test also
        // stays correct if upstream retry behavior changes: every successful
        // commit is durable and visible, and no write is lost or torn.
        let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());
        assert!(successes >= 1, "at least one append must commit");

        let dataset = DatasetBuilder::from_uri(uri.as_str())
            .with_session(session.clone())
            .with_commit_handler(rename_commit_handler())
            .load()
            .await
            .unwrap();
        assert_eq!(
            dataset.latest_version_id().await.unwrap(),
            1 + successes as u64
        );
        assert_eq!(dataset.count_rows(None).await.unwrap(), 3 * (1 + successes));
    }
}
