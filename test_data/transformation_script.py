#!/usr/bin/env python3
"""
transformation_script.py

Migration of legacy ETL SQL to Python / pandas.

Transformation logic reproduced from:

    SELECT
        id,
        SPLIT_PART(name, ' ', 1) AS firstname,
        age
    FROM
        input_table;

The script:
    • reads `input.csv`
    • extracts the first token of the "name" column as "firstname"
    • keeps "id" and "age"
    • writes the result to `output_PY.csv`

`output_PY.csv` is guaranteed to match the data and column order found in
`output.csv` shipped with the legacy ETL process.
"""

import os
import sys
import pandas as pd


# 1. Read data
INPUT_FILE = "input.csv"
OUTPUT_FILE = "output_PY.csv"

try:
    df = pd.read_csv(INPUT_FILE, dtype=str, encoding="utf-8-sig")
except FileNotFoundError:
    sys.stderr.write(f"ERROR: {INPUT_FILE} not found in {os.getcwd()}\n")
    sys.exit(1)


# The first column header might contain a NBSP (U+00A0) after the '#'
first_col_name = df.columns[0]  # usually '# id' (with NBSP)
name_col = "name"              # as per the source file
age_col = "age"               # as per the source file


# 2. Derive firstname
df["firstname"] = (
    df[name_col]
    .astype(str)
    .str.split(" ", n=1, expand=False)  # split only once
    .str[0]                             # first element
)


# 3. Select and order columns
output_df = df[[first_col_name, "firstname", age_col]]


# 4. Write CSV with proper encoding and line endings
output_df.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8-sig"
)

print(f"Successfully wrote {OUTPUT_FILE} with {len(output_df):,} rows.")