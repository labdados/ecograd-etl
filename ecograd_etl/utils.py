import pandas as pd
import re
import sys
import unicodedata
import urllib.request
from sqlalchemy import create_engine
from zipfile import ZipFile

def add_db_table_columns(cols, col_type, db_con, sql_table, sql_schema="public"):
    add_cols = ", ".join(['ADD COLUMN "{}" {}'.format(col, col_type) for col in cols])
    db_con.execute(f"ALTER TABLE IF EXISTS {sql_schema}.{sql_table} {add_cols};")

def build_db_url(db, user, password, host, port, db_name):
    return("{}://{}:{}@{}:{}/{}".format(db, user, password, host, port, db_name))

def clean_col_name(col):
    FIXES = [(r"[ /:\º,?()\.-]", "_"), (r"['’]", "")]
    col_name = str(col).lower()
    for search, replace in FIXES:
        col_name = re.sub(search, replace, col_name)  # noqa: PD005
    col_name = "".join(item for item in str(col_name) if item.isalnum() or "_" in item)
    col_name = "".join(
        letter
        for letter in unicodedata.normalize("NFD", col_name)
        if not unicodedata.combining(letter)
    )
    col_name = re.sub("_+", "_", col_name)
    col_name = col_name.strip("_")
    return col_name

def clean_col_names(cols):
    return [clean_col_name(col) for col in cols]

def create_db_schema(db_con, sql_schema):
    print(f"Creating schema {sql_schema}")
    db_con.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_schema};")

def create_db_engine(db_url):
    print("Creating database engine")
    return create_engine(db_url, pool_recycle=3600)

def download_file(url, output_file):
    output_filename = url.split('/')[-1]
    urllib.request.urlretrieve(url, filename=output_file)

def drop_db_table(db_con, sql_table, sql_schema="public"):
    print(f"Dropping table {sql_schema}.{sql_table}")
    db_con.execute(f"DROP TABLE IF EXISTS {sql_schema}.{sql_table};")

def filter_dict_by_keys(my_dict, keys):
    return {k: my_dict[k] for k in keys if k in my_dict}

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

def parse_float(x):
   try:
       return float(str(x).replace(',', '.'))
   except Exception:
       return None

def main(args):
    download_file(args[0], args[1])

if __name__ == '__main__':
    main(sys.argv[1:])
