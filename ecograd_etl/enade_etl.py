from dotenv import load_dotenv
from sqlalchemy import create_engine
from utils import download_file
import os
import pandas as pd
import psycopg2
import sys
from zipfile import ZipFile

load_dotenv()

enade_urls = {
    2019: "http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip",
    2018: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2018.zip",
    2017: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_Enade_2017_portal_2018.10.09.zip",
    2016: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2016_versao_28052018.zip",
    2015: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2015.zip",
    2014: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2014.zip",
    2013: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2013.zip",
    2012: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2012.zip",
    2011: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2011.zip",
    2010: "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2010.zip"
}

def download_enade(url, output_file):
    print(f"Downloading {url} to {output_file}")
    download_file(url, output_file)

def connect_db(db_url):
    print("Connecting to database")
    db_engine = create_engine(db_url, pool_recycle=3600)
    return db_engine.connect()

def drop_enade_table(db_con, sql_table, sql_schema="public"):
    print(f"Dropping table {sql_schema}.{sql_table}")
    db_con.execute(f"DROP TABLE IF EXISTS {sql_schema}.{sql_table};")

def list_column_names(db_con, sql_table, sql_schema="public"):
    try:
        res = pd.read_sql(f"SELECT * FROM {sql_schema}.{sql_table} LIMIT 0;", db_con)
        return(res.columns)
    except:
        return pd.Index([])

def add_table_columns(cols, db_con, sql_table, sql_schema="public"):
    add_cols = ", ".join(["ADD COLUMN {}".format(x) for x in cols])
    db_con.execute(f"ALTER TABLE IF EXISTS {sql_schema}.{sql_table} {add_cols};")

def open_csv_file_from_zip(zip_file_name):
    zip_file = ZipFile(zip_file_name, "r")
    csv_file_name = [name for name in zip_file.namelist() if name.endswith('.txt')][0]
    return(zip_file.open(csv_file_name))

def load_enade(zip_file_name, db_con, sql_table, sql_schema="public"):
    print(f"Loading {zip_file_name} to {sql_schema}.{sql_table}")
    csv_file = open_csv_file_from_zip(zip_file_name)
    cur_cols = list_column_names(db_con, sql_table, sql_schema)
    for df in pd.read_csv(csv_file, delimiter = ";", chunksize=10000):
        new_cols = cur_cols.difference(df.columns)
        if new_cols:
            print(f"New columns found: {new_cols}")
            add_table_columns(new_cols, db_con, sql_table, sql_schema)
        df.to_sql(
            sql_table, 
            db_con,
            sql_schema,
            index=False,
            if_exists='append'
        )

def main(args):
    output_dir = "data"
    db_url = "postgresql://{}:{}@{}:{}/{}".format(
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DB")
    )
    sql_table = "microdados_enade"
    sql_schema = "enade"
    db_con = connect_db(db_url)
    drop_enade_table(db_con, sql_table, sql_schema)
    for year, url in enade_urls.items():
       output_file = os.path.join(output_dir, f"microdados_enade_{year}.zip")
       download_enade(url, output_file)
       load_enade(output_file, db_con, sql_table, sql_schema)

if __name__ == '__main__':
    main(sys.argv[1:])
