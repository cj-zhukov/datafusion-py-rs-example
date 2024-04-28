pub type Result<T> = core::result::Result<T, Error>;

use thiserror::Error;
use std::io::Error as IOError;
use pyo3::PyErr;
use pyo3_asyncio::err::RustPanic;
use pyo3::exceptions::{PyIOError, PyValueError};
use datafusion::error::DataFusionError;
use datafusion::arrow::error::ArrowError;
use parquet::errors::ParquetError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("custom error: `{0}`")]
    Custom(String),

    #[error("io error: `{0}`")]
    IOError(#[from] IOError),

    #[error("pyo3 error: `{0}`")]
    Pyo3Error(#[from] PyErr),

    #[error("pyvalue error: `{0}`")]
    PyValueError(#[from] PyValueError),

    #[error("arrow error: `{0}`")]
    ArrowError(#[from] ArrowError),

    #[error("datafusion error: `{0}`")]
    DatafusionError(#[from] DataFusionError),

    #[error("parquet error: `{0}`")]
    ParquetError(#[from] ParquetError),

    #[error("rust panic error: `{0}`")]
    RustPanic(#[from] RustPanic),
}

impl std::convert::From<Error> for PyErr {
    fn from(value: Error) -> Self {
        match &value {
            Error::IOError(_) => PyIOError::new_err(value.to_string()), // get nice Python errors that one got used to
            _ => PyValueError::new_err(value.to_string()),
        }
    }
}