import datafusion_py_rs_example
from data_utils import calc_foo
from datafusion import SessionContext

def main():
    # get data from rust
    batches = datafusion_py_rs_example.get_data()
    ctx = SessionContext()
    ctx.register_record_batches("t", [batches])
    df = ctx.sql("select * from t")
    print(df)

    # write to parquet file
    # batches = df.collect()
    # datafusion_py_rs_example.write_batches(batches, "foo.parquet")

    # datafusion df can be converted to pandas/polars/arrow
    # df_pd = df.to_pandas()
    # print(df_pd)
    # df_pl = df.to_polars()
    # print(df_pl)
    # df_arrow = df.to_arrow_table()
    # print(df_arrow)

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