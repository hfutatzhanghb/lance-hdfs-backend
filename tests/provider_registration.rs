// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "hdfs")]
#[test]
fn test_register_inserts_hdfs_provider() {
    let registry = lance_io::object_store::ObjectStoreRegistry::default();

    lance_hdfs_backend::register(&registry);

    assert!(registry.get_provider("hdfs").is_some());
}
