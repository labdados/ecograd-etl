import os
import pandas as pd
import sys
import unidecode

import urllib.request
from sqlalchemy import create_engine
from zipfile import ZipFile

def add_db_table_columns(cols, col_type, db_con, sql_table, sql_schema="public"):
    add_cols = ", ".join(["ADD COLUMN {} {}".format(col, col_type) for col in cols])
    db_con.execute(f"ALTER TABLE IF EXISTS {sql_schema}.{sql_table} {add_cols};")

def build_db_url(db, user, password, host, port, db_name):
    return("{}://{}:{}@{}:{}/{}".format(db, user, password, host, port, db_name))

def clean_col_names(columns):
    return(columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', ''))

def create_db_schema(db_con, sql_schema):
    print(f"Creating schema {sql_schema}")
    db_con.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_schema};")

def connect_db(db_url):
    print("Connecting to database")
    db_engine = create_engine(db_url, pool_recycle=3600)
    return db_engine.connect()

def download_file(url, output_file):
    output_filename = url.split('/')[-1]
    #output_path = os.path.join(output_dir, output_filename)
    urllib.request.urlretrieve(url, filename=output_file)

def drop_db_table(db_con, sql_table, sql_schema="public"):
    print(f"Dropping table {sql_schema}.{sql_table}")
    db_con.execute(f"DROP TABLE IF EXISTS {sql_schema}.{sql_table};")

def list_db_column_names(db_con, sql_table, sql_schema="public"):
    try:
        res = pd.read_sql(f"SELECT * FROM {sql_schema}.{sql_table} LIMIT 0;", db_con)
        return(res.columns)
    except:
        return pd.Index([])

def open_file_from_zip(zip_file_name, extension=".txt"):
    zip_file = ZipFile(zip_file_name, "r")
    file_name = [name for name in zip_file.namelist() if name.endswith(extension)][0]
    return(zip_file.open(file_name))

def main(args):
    download_file(args[0], args[1])

if __name__ == '__main__':
    main(sys.argv[1:])