from tpch_polars import *
import numpy as np
import pandas as pd
import polars as pl

import io

result = run_thread("q1",1, "../../resources/tpc-h/data/scale=0.05/partition=5")


for record in result:
    buf = io.StringIO(record)
    tmp = pl.read_json(buf)
    df = tmp.to_pandas()
    print(df)

