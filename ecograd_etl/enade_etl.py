from dotenv import load_dotenv
import utils
import os
import pandas as pd
import sqlalchemy
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
    utils.download_file(url, output_file)

def load_enade(zip_file_name, db_con, sql_table, sql_schema="public"):
    print(f"Loading {zip_file_name} to {sql_schema}.{sql_table}")
    csv_file = utils.open_file_from_zip(zip_file_name)
    cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    for df in pd.read_csv(csv_file, delimiter = ";", chunksize=10000, low_memory=False):
        new_cols = df.columns.difference(cur_cols)
        if not cur_cols.empty and not new_cols.empty:
            print(f"New columns found: {new_cols}")
            utils.add_db_table_columns(new_cols, "text", db_con, sql_table, sql_schema)
        df.to_sql(
            sql_table, 
            db_con,
            sql_schema,
            index=False,
            if_exists='append',
            dtype=sqlalchemy.types.Text
        )

def main(args):
    output_dir = "data"
    db_url = utils.build_db_url("postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
                                os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"),
                                os.getenv("POSTGRES_DB"))
    
    sql_table = "microdados_enade"
    sql_schema = "enade"
    db_con = utils.connect_db(db_url)
    utils.drop_db_table(db_con, sql_table, sql_schema)
    for year, url in enade_urls.items():
       output_file = os.path.join(output_dir, f"microdados_enade_{year}.zip")
       download_enade(url, output_file)
       load_enade(output_file, db_con, sql_table, sql_schema)

if __name__ == '__main__':
    main(sys.argv[1:])
