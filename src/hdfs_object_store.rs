// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

use bytes::Bytes;
use futures::FutureExt;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore as OSObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, RenameTargetMode, path::Path,
};
use object_store_opendal::{IntoSendFuture, OpendalStore};
use opendal::Operator;
use opendal::raw::percent_decode_path;

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
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
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

        self.operator
            .rename(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
            )
            .into_send()
            .await
            .map_err(|error| Self::format_opendal_error(error, to))
    }
}
