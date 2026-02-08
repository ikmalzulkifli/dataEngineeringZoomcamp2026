#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


# We define the schema logic outside the function as a constant
DTYPE = {
    "VendorID": "Int64", "passenger_count": "Int64", "trip_distance": "float64",
    "RatecodeID": "Int64", "store_and_fwd_flag": "string", "PULocationID": "Int64",
    "DOLocationID": "Int64", "payment_type": "Int64", "fare_amount": "float64",
    "extra": "float64", "mta_tax": "float64", "tip_amount": "float64",
    "tolls_amount": "float64", "improvement_surcharge": "float64",
    "total_amount": "float64", "congestion_surcharge": "float64"
}
PARSE_DATES = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, type=int, help='Year of the taxi data')
@click.option('--month', default=1, type=int, help='Month of the taxi data')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'
    # 1. Setup Connection
    # We use the variables passed in from the CLI arguments
    connection_url = f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(connection_url)

    # 2. Create Schema (Replace existing table)
    # We read 0 rows just to get the structure
    df_schema = pd.read_csv(url, nrows=0, dtype=DTYPE, parse_dates=PARSE_DATES)
    df_schema.to_sql(name=target_table, con=engine, if_exists='replace')
    print(f"Table '{target_table}' schema created.")

    # 3. Batch Ingestion
    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=100000
    )

    for df_chunk in tqdm(df_iter, desc="Ingesting data"):
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

if __name__ == "__main__":
    run()