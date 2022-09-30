from tpch_polars import *
import numpy as np
import pandas as pd

result = run_thread("q1",1, "../../resources/tpc-h/data/scale=0.05/partition=5")

df = pd.DataFrame(data=result)

print(df)