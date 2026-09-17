// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

use bytes::Bytes;
use futures::FutureExt;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::{
    CopyMode, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore as OSObjectStore, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, RenameOptions, RenameTargetMode, UpdateVersion, path::Path,
};
use object_store_014 as object_store14;
use object_store_014::{MultipartUpload as _, ObjectStore as _};
use object_store_opendal::{IntoSendFuture, OpendalStore};
use opendal::Operator;
use opendal::raw::percent_decode_path;

fn convert_error(error: object_store14::Error) -> Error {
    match error {
        object_store14::Error::Generic { store, source } => Error::Generic { store, source },
        object_store14::Error::NotFound { path, source } => Error::NotFound { path, source },
        object_store14::Error::NotSupported { source } => Error::NotSupported { source },
        object_store14::Error::AlreadyExists { path, source } => {
            Error::AlreadyExists { path, source }
        }
        object_store14::Error::Precondition { path, source } => {
            Error::Precondition { path, source }
        }
        object_store14::Error::NotModified { path, source } => Error::NotModified { path, source },
        object_store14::Error::NotImplemented {
            operation,
            implementer,
        } => Error::NotImplemented {
            operation,
            implementer,
        },
        error => Error::Generic {
            store: "opendal",
            source: Box::new(error),
        },
    }
}

fn convert_error_up(error: Error) -> object_store14::Error {
    object_store14::Error::Generic {
        store: "hdfs",
        source: Box::new(error),
    }
}

fn path14(path: &Path) -> object_store14::path::Path {
    object_store14::path::Path::from(path.as_ref())
}

fn path13(path: &object_store14::path::Path) -> Path {
    Path::from(path.as_ref())
}

fn payload14(payload: PutPayload) -> object_store14::PutPayload {
    payload.into_iter().collect()
}

fn range14(range: GetRange) -> object_store14::GetRange {
    match range {
        GetRange::Bounded(range) => object_store14::GetRange::Bounded(range),
        GetRange::Offset(offset) => object_store14::GetRange::Offset(offset),
        GetRange::Suffix(suffix) => object_store14::GetRange::Suffix(suffix),
    }
}

fn get_options14(options: GetOptions) -> object_store14::GetOptions {
    object_store14::GetOptions {
        if_match: options.if_match,
        if_none_match: options.if_none_match,
        if_modified_since: options.if_modified_since,
        if_unmodified_since: options.if_unmodified_since,
        range: options.range.map(range14),
        version: options.version,
        head: options.head,
        ..Default::default()
    }
}

fn update_version14(version: UpdateVersion) -> object_store14::UpdateVersion {
    object_store14::UpdateVersion {
        e_tag: version.e_tag,
        version: version.version,
    }
}

fn put_mode14(mode: PutMode) -> object_store14::PutMode {
    match mode {
        PutMode::Overwrite => object_store14::PutMode::Overwrite,
        PutMode::Create => object_store14::PutMode::Create,
        PutMode::Update(version) => object_store14::PutMode::Update(update_version14(version)),
    }
}

fn put_options14(options: PutOptions) -> object_store14::PutOptions {
    object_store14::PutOptions {
        mode: put_mode14(options.mode),
        ..Default::default()
    }
}

fn copy_mode14(mode: CopyMode) -> object_store14::CopyMode {
    match mode {
        CopyMode::Overwrite => object_store14::CopyMode::Overwrite,
        CopyMode::Create => object_store14::CopyMode::Create,
    }
}

fn copy_options14(options: CopyOptions) -> object_store14::CopyOptions {
    object_store14::CopyOptions {
        mode: copy_mode14(options.mode),
        ..Default::default()
    }
}

fn rename_mode14(mode: RenameTargetMode) -> object_store14::RenameTargetMode {
    match mode {
        RenameTargetMode::Overwrite => object_store14::RenameTargetMode::Overwrite,
        RenameTargetMode::Create => object_store14::RenameTargetMode::Create,
    }
}

fn rename_options14(options: RenameOptions) -> object_store14::RenameOptions {
    object_store14::RenameOptions {
        target_mode: rename_mode14(options.target_mode),
        ..Default::default()
    }
}

fn put_result13(result: object_store14::PutResult) -> PutResult {
    PutResult {
        e_tag: result.e_tag,
        version: result.version,
    }
}

fn object_meta13(meta: object_store14::ObjectMeta) -> ObjectMeta {
    ObjectMeta {
        location: path13(&meta.location),
        last_modified: meta.last_modified,
        size: meta.size,
        e_tag: meta.e_tag,
        version: meta.version,
    }
}

fn get_result13(result: object_store14::GetResult) -> GetResult {
    let object_store14::GetResultPayload::Stream(stream) = result.payload;

    GetResult {
        payload: GetResultPayload::Stream(stream.map_err(convert_error).boxed()),
        meta: object_meta13(result.meta),
        range: result.range,
        attributes: Default::default(),
    }
}

fn list_result13(result: object_store14::ListResult) -> ListResult {
    ListResult {
        common_prefixes: result.common_prefixes.iter().map(path13).collect(),
        objects: result.objects.into_iter().map(object_meta13).collect(),
    }
}

struct OpendalMultipartUpload14 {
    inner: Box<dyn object_store14::MultipartUpload>,
}

impl Debug for OpendalMultipartUpload14 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpendalMultipartUpload14")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait::async_trait]
impl MultipartUpload for OpendalMultipartUpload14 {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let future = self.inner.put_part(payload14(data));
        async move { future.await.map_err(convert_error) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner
            .complete()
            .await
            .map(put_result13)
            .map_err(convert_error)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await.map_err(convert_error)
    }
}

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
        self.inner
            .put_opts(&path14(location), payload14(bytes), put_options14(options))
            .await
            .map(put_result13)
            .map_err(convert_error)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let inner = self
            .inner
            .put_multipart_opts(
                &path14(location),
                object_store14::PutMultipartOptions::default(),
            )
            .await
            .map_err(convert_error)?;

        Ok(Box::new(OpendalMultipartUpload14 { inner }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner
            .get_opts(&path14(location), get_options14(options))
            .await
            .map(get_result13)
            .map_err(convert_error)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner
            .get_ranges(&path14(location), ranges)
            .await
            .map_err(convert_error)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let converted = locations
            .map(|result| result.map(|path| path14(&path)).map_err(convert_error_up))
            .boxed();

        inner
            .delete_stream(converted)
            .map(|result| result.map(|path| path13(&path)).map_err(convert_error))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(path14);
        inner
            .list(prefix.as_ref())
            .map(|result| result.map(object_meta13).map_err(convert_error))
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(path14);
        inner
            .list_with_offset(prefix.as_ref(), &path14(offset))
            .map(|result| result.map(object_meta13).map_err(convert_error))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.map(path14);
        self.inner
            .list_with_delimiter(prefix.as_ref())
            .await
            .map(list_result13)
            .map_err(convert_error)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner
            .copy_opts(&path14(from), &path14(to), copy_options14(options))
            .await
            .map_err(convert_error)
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        if !matches!(options.target_mode, RenameTargetMode::Create) {
            return self
                .inner
                .rename_opts(&path14(from), &path14(to), rename_options14(options))
                .await
                .map_err(convert_error);
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
