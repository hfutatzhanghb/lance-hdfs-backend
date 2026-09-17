// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;

use lance_core::error::{Error, Result};
use lance_io::object_store::StorageOptions;
use url::Url;

pub(crate) fn build_config<I, K, V>(
    base_path: &Url,
    storage_options: &StorageOptions,
    env_vars: I,
) -> Result<HashMap<String, String>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    base_path
        .host_str()
        .ok_or_else(|| Error::invalid_input("HDFS URI must contain namenode host"))?;

    let env_vars = env_vars
        .into_iter()
        .filter_map(|(key, value)| {
            let value = value.into();
            if value.is_empty() {
                None
            } else {
                Some((key.as_ref().to_string(), value))
            }
        })
        .collect::<HashMap<_, _>>();

    let name_node = storage_options
        .0
        .get("hdfs_name_node")
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| env_vars.get("HDFS_NAME_NODE").cloned())
        .unwrap_or_else(|| format!("hdfs://{}", base_path.authority()));

    let mut config = HashMap::from([
        ("name_node".to_string(), name_node),
        ("root".to_string(), "/".to_string()),
        ("rename_overwrite".to_string(), "false".to_string()),
    ]);

    let user = storage_options
        .0
        .get("hdfs_user")
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| env_vars.get("HADOOP_USER_NAME").cloned())
        .or_else(|| env_vars.get("HDFS_USER").cloned());
    if let Some(user) = user {
        config.insert("user".to_string(), user);
    }

    for (storage_key, config_key) in [
        (
            "hdfs_kerberos_ticket_cache_path",
            "kerberos_ticket_cache_path",
        ),
        ("hdfs_atomic_write_dir", "atomic_write_dir"),
    ] {
        if let Some(value) = storage_options
            .0
            .get(storage_key)
            .filter(|value| !value.is_empty())
        {
            config.insert(config_key.to_string(), value.clone());
        }
    }

    Ok(config)
}

pub(crate) fn calculate_object_store_prefix_with_env(
    url: &Url,
    storage_options: Option<&HashMap<String, String>>,
    env_vars: &HashMap<String, String>,
) -> Result<String> {
    let authority = storage_options
        .and_then(|options| options.get("hdfs_name_node"))
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| env_vars.get("HDFS_NAME_NODE").cloned())
        .unwrap_or_else(|| url.authority().to_string());

    Ok(format!("{}${}", url.scheme(), authority))
}
