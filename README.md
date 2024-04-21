Datafusion examples how to use Rust with bindings to Python with maturin, pyo3, pyo3-asyncio

Main idea here is to do complicated work in Rust and pass results to Python. 
Results to Python can be passed by collecting datafusion dataframe in Rust into vec<RecordBatch>.
RecordBatch in Rust in arrow crate has traits FromPyArrow and ToPyArrow implimantations 
https://docs.rs/arrow/latest/arrow/array/struct.RecordBatch.html#impl-ToPyArrow-for-RecordBatch
