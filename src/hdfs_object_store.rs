// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::time::SystemTime;

use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryStreamExt, future, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore as OSObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, RenameTargetMode, path::Path,
};
use object_store_opendal::{IntoSendFuture, OpendalStore};
use opendal::raw::percent_decode_path;
use opendal::{Metadata, Operator};

pub(crate) struct HdfsObjectStore {
    inner: OpendalStore,
    operator: Operator,
}

impl HdfsObjectStore {
    pub(crate) fn new(operator: Operator) -> Self {
        Self {
            inner: OpendalStore::new(operator.clone()),
            operator,
        }
    }

    fn format_opendal_error(error: opendal::Error, path: &Path) -> object_store::Error {
        match error.kind() {
            opendal::ErrorKind::NotFound => object_store::Error::NotFound {
                path: path.to_string(),
                source: Box::new(error),
            },
            opendal::ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
                path: path.to_string(),
                source: Box::new(error),
            },
            opendal::ErrorKind::Unsupported => object_store::Error::NotSupported {
                source: Box::new(error),
            },
            opendal::ErrorKind::ConditionNotMatch => object_store::Error::Precondition {
                path: path.to_string(),
                source: Box::new(error),
            },
            kind => object_store::Error::Generic {
                store: kind.into_static(),
                source: Box::new(error),
            },
        }
    }

    /// Builds the operator-relative path of a listing request.
    ///
    /// `Path` drops trailing slashes, but OpenDAL needs one to tell a directory
    /// prefix apart from a plain key prefix.
    fn listing_path(prefix: Option<&Path>) -> String {
        prefix.map_or_else(String::new, |prefix| {
            format!("{}/", percent_decode_path(prefix.as_ref()))
        })
    }

    /// Converts a listed path and its OpenDAL metadata into object metadata, or
    /// `None` when the entry is not an object.
    ///
    /// HDFS directories are real entries in an OpenDAL listing, while
    /// `ObjectStore::list` is documented to yield objects only. Reporting a
    /// directory as a zero-byte object would hand callers a path whose delete
    /// removes a directory instead of a file.
    fn object_meta(path: &str, metadata: &Metadata) -> Option<ObjectMeta> {
        if metadata.is_dir() {
            return None;
        }

        Some(ObjectMeta {
            location: Path::from(path),
            last_modified: metadata
                .last_modified()
                .map_or_else(Default::default, |timestamp| {
                    SystemTime::from(timestamp).into()
                }),
            size: metadata.content_length(),
            e_tag: metadata.etag().map(str::to_string),
            version: metadata.version().map(str::to_string),
        })
    }

    /// Keeps only the child directories of a delimited listing.
    ///
    /// OpenDAL reports the listed directory itself as an entry, which
    /// `ListResult::common_prefixes` must not contain.
    fn child_prefixes(common_prefixes: &mut Vec<Path>, prefix: Option<&Path>) {
        common_prefixes
            .retain(|path| !path.as_ref().is_empty() && prefix.is_none_or(|prefix| path != prefix));
    }
}

impl Debug for HdfsObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HdfsObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl Display for HdfsObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.inner)
    }
}

#[async_trait::async_trait]
impl OSObjectStore for HdfsObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = Self::listing_path(prefix);
        let error_path = Path::from(path.as_str());
        let operator = self.operator.clone();

        let listing = async move {
            let lister = operator
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|error| Self::format_opendal_error(error, &error_path))?;

            Ok::<_, object_store::Error>(lister.filter_map(move |result| {
                let error_path = error_path.clone();
                async move {
                    match result {
                        Ok(entry) => Self::object_meta(entry.path(), entry.metadata()).map(Ok),
                        Err(error) => Some(Err(Self::format_opendal_error(error, &error_path))),
                    }
                }
            }))
        };

        listing.into_send().into_stream().try_flatten().boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // The HDFS service cannot push `start_after` down to the name node, so
        // the exclusive offset is applied to the filtered listing.
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |meta| future::ready(meta.location > offset))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        Self::child_prefixes(&mut result.common_prefixes, prefix);
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        if !matches!(options.target_mode, RenameTargetMode::Create) {
            return self.inner.rename_opts(from, to, options).await;
        }

        // Create-mode rename is the dataset commit primitive: it must fail
        // instead of replacing an existing target. Request OpenDAL
        // if-not-exists semantics explicitly; without it the HDFS service
        // deletes an existing target and renames over it. The service reports
        // the conflict as ConditionNotMatch, which object_store requires to be
        // surfaced as AlreadyExists for RenameTargetMode::Create.
        self.operator
            .rename_with(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
            )
            .if_not_exists(true)
            .into_send()
            .await
            .map_err(|error| match error.kind() {
                opendal::ErrorKind::ConditionNotMatch => object_store::Error::AlreadyExists {
                    path: to.to_string(),
                    source: Box::new(error),
                },
                _ => Self::format_opendal_error(error, to),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt, stream};
    use object_store::{Error, GetRange, ObjectStoreExt};
    use opendal::{MetadataBuilder, raw::Timestamp};

    fn memory_store() -> HdfsObjectStore {
        HdfsObjectStore::new(Operator::new(opendal::services::Memory::default()).unwrap())
    }

    #[tokio::test]
    async fn get_preserves_range_and_object_metadata() {
        let store = memory_store();
        let path = Path::from("test/data");
        store
            .put(&path, Bytes::from_static(b"abcdef").into())
            .await
            .unwrap();

        let result = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(1..4)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(result.meta.location, path);
        assert_eq!(result.meta.size, 6);
        assert_eq!(result.range, 1..4);
        assert_eq!(result.bytes().await.unwrap(), Bytes::from_static(b"bcd"));
    }

    #[tokio::test]
    async fn delete_stream_preserves_input_error_kind() {
        let store = memory_store();
        let error = Error::NotFound {
            path: "missing".to_string(),
            source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
        };
        let result = store
            .delete_stream(stream::iter(vec![Err(error)]).boxed())
            .try_collect::<Vec<_>>()
            .await;

        assert!(matches!(result, Err(Error::NotFound { path, .. }) if path == "missing"));
    }

    #[test]
    fn object_meta_skips_directories_and_keeps_file_metadata() {
        let directory = MetadataBuilder::dir().build();
        assert!(HdfsObjectStore::object_meta("dataset/_versions/", &directory).is_none());

        let mut builder = MetadataBuilder::file(11);
        builder.last_modified(Timestamp::from_second(1_700_000_000).unwrap());
        let file = builder.build();

        let meta = HdfsObjectStore::object_meta("dataset/_versions/1.manifest", &file)
            .expect("files must be listed");
        assert_eq!(meta.location, Path::from("dataset/_versions/1.manifest"));
        assert_eq!(meta.size, 11);
        assert_eq!(meta.last_modified.timestamp(), 1_700_000_000);
    }

    #[test]
    fn child_prefixes_drops_the_listed_directory_itself() {
        let mut common_prefixes = vec![
            Path::from("dataset"),
            Path::from("dataset/_versions"),
            Path::from("dataset/data"),
        ];

        HdfsObjectStore::child_prefixes(&mut common_prefixes, Some(&Path::from("dataset")));

        assert_eq!(
            common_prefixes,
            vec![Path::from("dataset/_versions"), Path::from("dataset/data")]
        );
    }

    #[test]
    fn child_prefixes_drops_the_root_when_listing_without_a_prefix() {
        let mut common_prefixes = vec![Path::from("/"), Path::from("dataset")];

        HdfsObjectStore::child_prefixes(&mut common_prefixes, None);

        assert_eq!(common_prefixes, vec![Path::from("dataset")]);
    }
}
