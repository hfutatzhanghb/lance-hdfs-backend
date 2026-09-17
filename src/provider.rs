// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;

use lance_core::error::{Error, Result};
use lance_io::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, ObjectStore, ObjectStoreParams, ObjectStoreProvider,
    StorageOptions,
};
use opendal::{Operator, services::Hdfs};
use url::Url;

use crate::config::{build_config, calculate_object_store_prefix_with_env};
use crate::hdfs_object_store::HdfsObjectStore;

/// HDFS object store provider backed by OpenDAL.
#[derive(Debug)]
pub struct HdfsStoreProvider;

impl HdfsStoreProvider {
    fn operator_error(error: impl std::fmt::Display, name_node: &str, has_user: bool) -> Error {
        Error::io(format!(
            "Failed to create HDFS operator: {error}. name_node={name_node}, has_user={has_user}"
        ))
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for HdfsStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let config = build_config(&base_path, &storage_options, std::env::vars())?;

        let name_node = config
            .get("name_node")
            .cloned()
            .unwrap_or_else(|| "<missing>".to_string());
        let has_user = config.contains_key("user");
        let operator = Operator::from_iter::<Hdfs>(config)
            .map_err(|error| Self::operator_error(error, &name_node, has_user))?
            .finish();

        let store_prefix =
            self.calculate_object_store_prefix(&base_path, params.storage_options())?;
        let download_retry_count = storage_options.download_retry_count();
        let list_is_lexically_ordered = params.list_is_lexically_ordered.unwrap_or(false);
        let mut store = ObjectStore::new(
            std::sync::Arc::new(HdfsObjectStore::new(operator)),
            base_path,
            params.block_size,
            None,
            params.use_constant_size_upload_parts,
            list_is_lexically_ordered,
            DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            params.storage_options(),
        );
        store.store_prefix = store_prefix;

        Ok(store)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let env_vars = std::env::vars().collect::<HashMap<String, String>>();
        calculate_object_store_prefix_with_env(url, storage_options, &env_vars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_io::object_store::ObjectStoreProvider;
    use object_store::path::Path;

    #[test]
    fn test_hdfs_store_paths() {
        let provider = HdfsStoreProvider;
        let cases = [
            ("hdfs://namenode:9000/path/to/file", "path/to/file"),
            ("hdfs://namenode/path/to/file", "path/to/file"),
            ("hdfs://namenode:9000/", ""),
            (
                "hdfs://namenode:9000/user/data/dataset/file.parquet",
                "user/data/dataset/file.parquet",
            ),
            ("hdfs://ht-hdfsqa/user/data/file.txt", "user/data/file.txt"),
        ];

        for (url, expected_path) in cases {
            let path = provider.extract_path(&Url::parse(url).unwrap()).unwrap();
            assert_eq!(path, Path::from(expected_path));
        }
    }

    #[test]
    fn test_hdfs_config_from_url() {
        let url = Url::parse("hdfs://namenode:9000/test").unwrap();
        let config =
            build_config(&url, &StorageOptions::default(), Vec::<(&str, &str)>::new()).unwrap();

        assert_eq!(config.get("name_node").unwrap(), "hdfs://namenode:9000");
        assert_eq!(config.get("root").unwrap(), "/");
        assert_eq!(config.get("rename_overwrite").unwrap(), "false");
    }

    #[test]
    fn test_hdfs_storage_options_override_environment_and_url() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let storage_options = StorageOptions(HashMap::from([
            (
                "hdfs_name_node".to_string(),
                "hdfs://option-namenode:8020".to_string(),
            ),
            ("hdfs_user".to_string(), "option-user".to_string()),
            (
                "hdfs_kerberos_ticket_cache_path".to_string(),
                "/tmp/krb5cc".to_string(),
            ),
            (
                "hdfs_atomic_write_dir".to_string(),
                "/tmp/atomic".to_string(),
            ),
        ]));
        let env_vars = [
            ("HDFS_NAME_NODE", "hdfs://env-namenode:9000"),
            ("HADOOP_USER_NAME", "env-user"),
        ];

        let config = build_config(&url, &storage_options, env_vars).unwrap();

        assert_eq!(
            config.get("name_node").unwrap(),
            "hdfs://option-namenode:8020"
        );
        assert_eq!(config.get("user").unwrap(), "option-user");
        assert_eq!(
            config.get("kerberos_ticket_cache_path").unwrap(),
            "/tmp/krb5cc"
        );
        assert_eq!(config.get("atomic_write_dir").unwrap(), "/tmp/atomic");
        assert_eq!(config.get("rename_overwrite").unwrap(), "false");
    }

    #[test]
    fn test_hdfs_config_from_environment() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let env_vars = [
            ("HDFS_NAME_NODE", "hdfs://env-namenode:9000"),
            ("HADOOP_USER_NAME", "env-user"),
        ];

        let config = build_config(&url, &StorageOptions::default(), env_vars).unwrap();

        assert_eq!(config.get("name_node").unwrap(), "hdfs://env-namenode:9000");
        assert_eq!(config.get("user").unwrap(), "env-user");
    }

    #[test]
    fn test_hdfs_config_rejects_url_without_host() {
        let url = Url::parse("hdfs:///test").unwrap();
        let error =
            build_config(&url, &StorageOptions::default(), Vec::<(&str, &str)>::new()).unwrap_err();

        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("namenode host"));
    }

    #[test]
    fn test_hdfs_operator_error_includes_connection_context() {
        let error = HdfsStoreProvider::operator_error(
            std::io::Error::other("native client unavailable"),
            "hdfs://namenode:9000",
            true,
        );
        let message = error.to_string();

        assert!(matches!(error, Error::IO { .. }));
        assert!(message.contains("native client unavailable"));
        assert!(message.contains("name_node=hdfs://namenode:9000"));
        assert!(message.contains("has_user=true"));
    }

    #[test]
    fn test_hdfs_store_prefix_uses_effective_name_node() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let storage_options = HashMap::from([(
            "hdfs_name_node".to_string(),
            "hdfs://option-namenode:8020".to_string(),
        )]);
        let env_vars = HashMap::from([(
            "HDFS_NAME_NODE".to_string(),
            "hdfs://env-namenode:9000".to_string(),
        )]);

        let prefix =
            calculate_object_store_prefix_with_env(&url, Some(&storage_options), &env_vars)
                .unwrap();

        assert_eq!(prefix, "hdfs$hdfs://option-namenode:8020");
    }
}
