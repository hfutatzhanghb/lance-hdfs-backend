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

    use lance_hdfs_backend::register;
    use lance_io::object_store::{
        ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
    };
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, RenameOptions, RenameTargetMode};

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
        let (store, path) = ObjectStore::from_uri_and_params(
            registry(),
            "hdfs://ht-hdfsqa/user/test",
            &Default::default(),
        )
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
}
