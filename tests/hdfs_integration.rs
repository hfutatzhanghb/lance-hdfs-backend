// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Integration tests for the HDFS object store provider.
//!
//! These tests require a live HDFS cluster and are ignored by default.

#[cfg(feature = "hdfs")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use lance_hdfs_backend::register;
    use lance_io::object_store::{
        ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
    };
    use object_store::ObjectStore as OSObjectStore;
    use object_store::path::Path;

    fn registry() -> Arc<ObjectStoreRegistry> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        register(&registry);
        registry
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
                "hdfs://namenode:9000".to_string(),
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
