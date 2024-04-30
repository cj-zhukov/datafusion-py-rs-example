#  datafusion-py-rs-example
This is a Rust library shows how to empower your Python data pipelines with Rust.

## Description
Project example how to use Rust with binding to Python with datafusion, maturin, pyo3, pyo3-asyncio.
Main idea here is to do complicated work in Rust and pass results to Python. 
Results to Python can be passed by collecting datafusion dataframe in Rust into vector of RecordBatch.
RecordBatch in Rust in arrow crate has traits FromPyArrow https://docs.rs/arrow/latest/arrow/array/struct.RecordBatch.html#impl-FromPyArrow-for-RecordBatch
and ToPyArrow implemantations https://docs.rs/arrow/latest/arrow/array/struct.RecordBatch.html#impl-ToPyArrow-for-RecordBatch
RecordBatch type is not the only type that can be passed, see more https://docs.rs/arrow/latest/arrow/pyarrow/index.html

## Usage
1) setup virt enviroment in Python
2) install maturin and others libs in Python (datafusion, pandas, polars)
```python
  pip install maturin
```
3) build your Rust crate into wheel package and install it in Python virt env
```python
  maturin develop
```
4) call Rust from Python
```python
  import datafusion_py_rs_example
  from datafusion import SessionContext

  batches = datafusion_py_rs_example.run()
  ctx = SessionContext()
  ctx.register_record_batches("foo", [batches])
  df = ctx.sql("select * from foo")
  print(df)
````

## Installation
Use provided dockerfile for building Rust crate into wheel package and installing it with ather Python libs