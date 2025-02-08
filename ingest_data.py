#!/usr/bin/env python
# coding: utf-8
import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url

    # Download the dataset file
    file_name = "output.csv"
    os.system(f"wget {url} -O {file_name}")
    
    # Create database engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()
    # Read in the dataset from parquet format
    df = pd.read_parquet(file_name, engine="pyarrow")

    # Create an empty table with only header row
    df.head(0).to_sql(name=table, con=engine, if_exists="replace")
    # Write the rest of the file, by appending to the existing table
    df.to_sql(name=table, con=engine, if_exists="append")

if __name__ == "__main__":
    # Define parser and its arguments
    parser = argparse.ArgumentParser(description="Ingest PARQUET data to Postgres")

    # Arguments needed for parser
    # user, password, host, port, database name, table name and url of PARQUET file
    parser.add_argument("user", help="username for Postgres")
    parser.add_argument("password", help="password for Postgres")
    parser.add_argument("host", help="host for Postgres")
    parser.add_argument("port", help="port for Postgres")
    parser.add_argument("db", help="database name")
    parser.add_argument("table", help="name of the table where results are written to")
    parser.add_argument("url", help="url of the dataset")

    args = parser.parse_args()

    main(args)







