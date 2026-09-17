// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::{
    DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE, WriteMode, WriteParams,
    builder::DatasetBuilder,
};
use lance::session::Session;
use lance_hdfs_backend::{register, rename_commit_handler};
use lance_io::object_store::ObjectStoreRegistry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "hdfs://localhost:9000/example-dataset".to_string());
    let registry = Arc::new(ObjectStoreRegistry::default());
    register(&registry);
    let session = Arc::new(Session::new(
        DEFAULT_INDEX_CACHE_SIZE,
        DEFAULT_METADATA_CACHE_SIZE,
        registry,
    ));

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from(vec![1, 2, 3]))],
    )?;
    let batches = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());

    Dataset::write(
        batches,
        uri.as_str(),
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            session: Some(session.clone()),
            commit_handler: Some(rename_commit_handler()),
            ..Default::default()
        }),
    )
    .await?;

    let dataset = DatasetBuilder::from_uri(uri.as_str())
        .with_session(session.clone())
        .with_commit_handler(rename_commit_handler())
        .load()
        .await?;
    println!("rows={}", dataset.count_rows(None).await?);

    Ok(())
}
