use std::sync::Arc;

use anyhow::Context;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray, Int32Array};
use datafusion::arrow::pyarrow;
use datafusion::parquet::arrow::AsyncArrowWriter;
use pyo3::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

#[pyfunction]
fn get_data(py: Python<'_>) -> PyResult<pyarrow::PyArrowType<Vec<RecordBatch>>> {
    let batches = pyo3_asyncio::tokio::run(py, async move {
        let df = get_df().await.unwrap();
        let batches = df.collect().await.unwrap();
        Ok(batches)
    });
    let res = batches.unwrap();

    Ok(res.into())
}

#[pyfunction]
fn process_data(py: Python<'_>, batches: pyarrow::PyArrowType<Vec<RecordBatch>>) -> PyResult<()>{
    let _res = pyo3_asyncio::tokio::run(py, async move {
        process_batches(batches.0).await.unwrap();
        Ok(())
    }); 

    Ok(())
}

#[pyfunction]
fn write_batches(py: Python<'_>, batches: pyarrow::PyArrowType<Vec<RecordBatch>>, file_path: String) -> PyResult<()>{
    let _res = pyo3_asyncio::tokio::run(py, async move {
        write_batches_to_file(batches.0, &file_path).await.unwrap();
        Ok(())
    }); 

    Ok(())
}

#[pymodule]
fn datafusion_py_rs_example(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_data, m)?)?;
    m.add_function(wrap_pyfunction!(write_batches, m)?)?;
    m.add_function(wrap_pyfunction!(process_data, m)?)?;
    Ok(())
}

pub async fn get_df() -> anyhow::Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub async fn process_batches(batches: Vec<RecordBatch>) -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_batches(batches)?;
    df.show().await?;

    Ok(())
}

pub async fn write_df_to_file(df: DataFrame, file_path: &str) -> anyhow::Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await.context("could not create stream from df")?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None).context("could not create writer")?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let mut file = tokio::fs::File::create(file_path).await?;
    file.write_all(&mut buf).await?;

    Ok(())
}

async fn write_batches_to_file(batches: Vec<RecordBatch>, file_path: &str) -> anyhow::Result<()> {
    let mut buf = vec![];
    let schema = batches[0].schema();
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema, None).context("could not create writer")?;
    for batch in batches {
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let mut file = tokio::fs::File::create(file_path).await?;
    file.write_all(&mut buf).await?;

    Ok(())
}