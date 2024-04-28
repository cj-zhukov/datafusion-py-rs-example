pub mod error;
use error::Result;

use std::sync::Arc;

use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray, Int32Array};
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::parquet::arrow::AsyncArrowWriter;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

#[pyfunction]
fn get_data(py: Python<'_>, input: i32) -> PyResult<PyArrowType<Vec<RecordBatch>>> {
    let batches = pyo3_asyncio::tokio::run(py, async move {
        let df = match input {
            1 => {
                get_df1().await
                    .map_err(|e| PyValueError::new_err(format!("failed running get_data1 cause: {}", e)))?
            }
            2 => {
                get_df2().await
                    .map_err(|e| PyValueError::new_err(format!("failed running get_data2 cause: {}", e)))?
            }
            _ => {
                println!("unknown mode is chosen, getting empty df");
                let ctx = SessionContext::new();
                ctx.read_empty()
                    .map_err(|e| PyValueError::new_err(format!("failed running get_data cause: {}", e)))?
            }
        };

        Ok(df.collect().await?)
    });

    Ok(batches?.into())
}

#[pyfunction]
fn run(py: Python<'_>) -> PyResult<PyArrowType<Vec<RecordBatch>>> {
    let batches = pyo3_asyncio::tokio::run(py, async move {
        let df1 = get_df1().await
            .map_err(|e| PyValueError::new_err(format!("failed running get_data1 cause: {}", e)))?;
        let df2 = get_df2().await
            .map_err(|e| PyValueError::new_err(format!("failed running get_data2 cause: {}", e)))?
            .with_column_renamed("id", "id2")?;
        let res = df1.join(df2, JoinType::Inner, &["id"], &["id2"], None)?
            .select_columns(&["id", "name", "data"])?;
    
        Ok(res.collect().await?)
    });

    Ok(batches?.into())
}

#[pyfunction]
fn process_data(py: Python<'_>, batches: PyArrowType<Vec<RecordBatch>>) -> PyResult<()>{
    let _res = pyo3_asyncio::tokio::run(py, async move {
        process_batches(batches.0).await
            .map_err(|e| PyValueError::new_err(format!("failed running process_batches cause: {}", e)))?;
        Ok(())
    });

    Ok(())
}

#[pyfunction]
fn write_batches(py: Python<'_>, batches: PyArrowType<Vec<RecordBatch>>, file_path: String) -> PyResult<()>{
    let _res = pyo3_asyncio::tokio::run(py, async move {
        write_batches_to_file(batches.0, &file_path).await
            .map_err(|e| PyValueError::new_err(format!("failed running write_batches cause: {}", e)))?;
        Ok(())
    }); 

    Ok(())
}

#[pymodule]
fn datafusion_py_rs_example(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_data, m)?)?;
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(write_batches, m)?)?;
    m.add_function(wrap_pyfunction!(process_data, m)?)?;
    Ok(())
}

pub async fn get_df1() -> Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;

    Ok(df)
}

pub async fn get_df2() -> Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;

    Ok(df)
}

pub async fn process_batches(batches: Vec<RecordBatch>) -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_batches(batches)?;
    df.show().await?;

    Ok(())
}

pub async fn write_df_to_file(df: DataFrame, file_path: &str) -> Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let mut file = tokio::fs::File::create(file_path).await?;
    file.write_all(&mut buf).await?;

    Ok(())
}

async fn write_batches_to_file(batches: Vec<RecordBatch>, file_path: &str) -> Result<()> {
    let mut buf = vec![];
    let schema = batches[0].schema();
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema, None)?;
    for batch in batches {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let mut file = tokio::fs::File::create(file_path).await?;
    file.write_all(&mut buf).await?;

    Ok(())
}