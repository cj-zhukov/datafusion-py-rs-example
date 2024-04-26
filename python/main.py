import datafusion_py_rs_example
from data_utils import calc_foo
from datafusion import SessionContext

def main():
    # process data
    batches = datafusion_py_rs_example.run()
    ctx = SessionContext()
    ctx.register_record_batches("foo", [batches])
    df = ctx.sql("select * from foo")
    print(df)

    # write to parquet file
    batches = df.collect()
    datafusion_py_rs_example.write_batches(batches, "foo.parquet")

    # get data
    batches = datafusion_py_rs_example.get_data(1) # 1 | 2
    ctx = SessionContext()
    ctx.register_record_batches("bar", [batches])
    df = ctx.sql("select * from bar")
    print(df)

    # datafusion df can be converted to pandas/polars/arrow
    df_pd = df.to_pandas()
    print(df_pd)
    df_pl = df.to_polars()
    print(df_pl)
    df_arrow = df.to_arrow_table()
    print(df_arrow)

    # perform some actions on datafusion df in python
    table = df.to_arrow_table()
    foo_cols = table.column("name").to_pylist()
    foo_col = [calc_foo(x) for x in foo_cols]
    table = table.add_column(0, 'foo', [foo_col])
    df = ctx.from_arrow_table(table)
    print(df)

    # pass result back to rust
    datafusion_py_rs_example.process_data(table.to_batches())


if __name__ == "__main__":
    main()