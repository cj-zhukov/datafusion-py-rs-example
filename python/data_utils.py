import datafusion
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

def calc_foo(x: str) -> str:
    return f"{x}_foo"


def add_col_example():
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["foo", "bar", "baz"])],
        names=["id", "name"],
    )
    ctx = datafusion.SessionContext()
    df = ctx.create_dataframe([[batch]])
    table = df.to_arrow_table()
    table = table.append_column('new-col', pa.array(['foo'] * len(table), pa.string()))
    print(table)


if __name__ == "__main__":
    ctx = datafusion.SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["foo", "bar", "baz"])],
        names=["id", "name"],
    )

    df = ctx.create_dataframe([[batch]])
    table = df.to_arrow_table()
    new_col = table.column("name").to_pylist()
    new_col = [calc_foo(x) for x in new_col]
    table = table.add_column(0, 'new-col', [new_col])
    batches = table.to_batches()
    for i, batch in enumerate(batches):
        print(f"#{i} {batch}")

    df = ctx.from_arrow_table(table)
    print(df)