import os
import sys
import glob
import polars as pl
from concurrent.futures import ThreadPoolExecutor
# added
# import pandas as pd
# Converts the filename from: %x.tbl.1 to %x.json.1
def convert_file_to_json(file_name):
    target_file_name = file_name.replace('tbl','json')
    if os.path.exists(target_file_name):
        print(f"File {target_file_name} already exists")
        return target_file_name
    # df = pl.read_csv(file_name, has_header = False, sep = "|")
    df = pl.read_csv(file_name, has_header = False, sep = "|")
    # print(df[:10])
    df.write_ndjson(target_file_name)
    return target_file_name

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 convert_to_json.py <input-dir. ex: ../sresources/tpc-h/data/scale=10/partition=10/tbl/>")
        exit(1)
    input_directory = sys.argv[1]

    # Obtain the files that need to be converted
    files = glob.glob(os.path.join(input_directory,"*.tbl*"))

    output_directory = input_directory.replace("tbl","json")
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)

    # Convert each file to json
    print(f"Found {len(files)} files")
    for file_name in files:
        print(f"Converting {file_name}")
        convert_file_to_json(file_name)
