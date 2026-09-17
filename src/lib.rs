// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![doc = include_str!("../README.md")]

pub const HDFS_SCHEME: &str = "hdfs";

#[cfg(feature = "hdfs")]
mod config;
#[cfg(feature = "hdfs")]
mod hdfs_object_store;
#[cfg(feature = "hdfs")]
mod provider;

#[cfg(feature = "hdfs")]
pub use provider::HdfsStoreProvider;

#[cfg(feature = "hdfs")]
use std::sync::Arc;

#[cfg(feature = "hdfs")]
use lance_io::object_store::ObjectStoreRegistry;

/// Registers the HDFS object store provider in a Lance registry.
///
/// This only registers object-store resolution for the `hdfs` scheme. Lance's
/// commit-handler selection is independent of [`ObjectStoreRegistry`], so
/// writers must also call [`rename_commit_handler`] and pass the returned
/// handler to Lance.
#[cfg(feature = "hdfs")]
pub fn register(registry: &ObjectStoreRegistry) {
    registry.insert(HDFS_SCHEME, Arc::new(HdfsStoreProvider));
}

/// Returns the commit handler recommended for HDFS datasets.
///
/// HDFS supports atomic renames, so [`RenameCommitHandler`] is used instead of
/// the unsafe unknown-scheme fallback in Lance 8.0.0.
///
/// [`RenameCommitHandler`]: lance_table::io::commit::RenameCommitHandler
#[cfg(feature = "commit-handler")]
pub fn rename_commit_handler() -> Arc<dyn lance_table::io::commit::CommitHandler> {
    Arc::new(lance_table::io::commit::RenameCommitHandler)
}
