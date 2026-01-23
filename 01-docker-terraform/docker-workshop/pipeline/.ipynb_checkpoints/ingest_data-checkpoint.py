#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
@click.option('--color', default='yellow', help='Target taxi cluster')
@click.option('--year', default=1000, help='The year of the data')
@click.option('--month', default=0, help='The month of the data')
@click.option('--chunksize', default=100000, help='The size of each chunk to iterately insert data to SQL tables')

def ingest_data(user, password, host, port, db, table, color, year, month, chunksize):

    path = f'/home/moqian/workspace/de-zoomcamp-work/docker-workshop/pipeline/{color}_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

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

    parse_dates = [
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime"
    ]

    df_iter = pd.read_csv(path,
        dtype=dtype,
        parse_dates=parse_dates,
        date_format='%Y-%m-%d %H:%M:%S',
        iterator=True,
        chunksize=chunksize)

    # Insert values in chunks of equal size (except for the last chunk which gets the modulo from size divided by chunk size)
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=table, con=engine, if_exists='replace')
            first = False
            print("Table created")
        df_chunk.to_sql(name=table, con=engine, if_exists='append')
        print("Inserted: ", len(df_chunk))

if __name__ == '__main__':
    ingest_data()