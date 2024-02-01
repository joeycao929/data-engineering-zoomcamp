#!/usr/bin/env python
import argparse
import pandas as pd
from sqlalchemy import create_engine
import os



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name

    # download the csv
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


    # This will only create table, but not insert any data into db
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')




    while True:
        try:
            df = next(df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
        except StopIteration:
            print('Complete')
            break

def lookup_zone(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df = pd.read_csv(csv_name)

    # This will only create table, but not insert any data into db
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest csv data to Postgres")

    #user, password, host, port, database name, table name, url of the csv
    parser.add_argument('--user', help='username for pg')
    parser.add_argument('--password', help='password for pg')
    parser.add_argument('--host', help='host for pg')
    parser.add_argument('--port', help='port for pg')
    parser.add_argument('--db', help='database for pg')
    parser.add_argument('--table_name', help='name of the table where we will write the resutlts to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)
    # lookup_zone(args)






