#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# --- 1. CONFIGURATION BLOCK ---
# Define these first so the code below can see them
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
year = 2021
month = 1
url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'
target_table = 'yellow_taxi_data'

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

# --- 2. SCHEMA INITIALIZATION BLOCK ---
engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

# Read just 100 rows to get the column headers
df_sample = pd.read_csv(
    url,
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)

# Create the table schema (empty)
df_sample.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
print("Table schema created successfully.")

# --- 3. EXECUTION BLOCK ---
def run(): # Added the colon here
    # Use the URL defined above
    chunksize = 100000

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    # Note: Use if_exists='append' here
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')
        # Removed the typo 'if_exist'

# Call the function to actually start the process
if __name__ == "__main__":
    run()