Datafusion examples how to use Rust with bindings to Python with maturin, pyo3, pyo3-asyncio

Main idea here is to do complicated work in Rust and pass results to Python. 
Results to Python can be passed by collecting datafusion dataframe in Rust into vector of RecordBatch.
RecordBatch in Rust in arrow crate has traits FromPyArrow https://docs.rs/arrow/latest/arrow/array/struct.RecordBatch.html#impl-FromPyArrow-for-RecordBatch
and ToPyArrow implemantations https://docs.rs/arrow/latest/arrow/array/struct.RecordBatch.html#impl-ToPyArrow-for-RecordBatch
RecordBatch type is not the only type that can be passed, see more https://docs.rs/arrow/latest/arrow/pyarrow/index.html

How to do it locally:
1) setup virt enviroment in Python
2) install maturin and others libs in Python (datafusion, pandas, polars):
  pip install maturin
3) build your Rust crate into wheel package and install it in Python virt env:
  maturin develop
4) call Rust from Python

There is Dockerfile provided:
1) build Rust crate into wheel package for Linux
2) requirements.txt uses that build Rust crate to install it locally
