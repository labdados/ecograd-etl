from io import BytesIO
import os
import pandas as pd
import re
import sys
import unicodedata
import urllib.request
from rarfile import RarFile
from sqlalchemy import create_engine
from zipfile import ZipFile

from sqlalchemy.sql.sqltypes import NullType

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
    col_name = remove_accents(col_name)
    col_name = re.sub("_+", "_", col_name)
    col_name = col_name.strip("_")
    return col_name

def clean_db(db_con, sql_table, sql_schema):
    create_db_schema(db_con, sql_schema)
    drop_db_table(db_con, sql_table, sql_schema)

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

def get_file_extension(filename):
    return os.path.splitext(filename)[1]

def load_csv_to_db(csv_file, db_con, sql_table, sql_schema):
    df = pd.read_csv(csv_file)
    load_dataframe_to_db(sql_table, db_con, sql_schema, if_exists='replace')

def load_dataframe_to_db(df, db_con, db_table, db_schema, if_exists='replace'):
    df.to_sql(db_table, db_con, db_schema, index=False, if_exists=if_exists)

def list_db_column_names(db_con, sql_table, sql_schema="public"):
    try:
        res = pd.read_sql(f"SELECT * FROM {sql_schema}.{sql_table} LIMIT 0;", db_con)
        return(res.columns)
    except:
        return pd.Index([])

def open_file_from_zip(zip_file_name, file_regex=""):
    compressed_file = ZipFile(zip_file_name, "r")
    while(True):
        regex = re.compile(f".*{file_regex}.*", re.IGNORECASE)
        files = filter(regex.match, compressed_file.namelist())
        file_name = next(files, None)
        if not file_name:
            return None
        print(f"Opening {file_name}")
        file_extension = get_file_extension(file_name).lower()
        file = compressed_file.open(file_name)
        if file_extension == ".zip":
            compressed_file = ZipFile(file)
        elif file_extension == ".rar":
            compressed_file = RarFile(BytesIO(file.read()))
        else:
            return file
            

def parse_float(x):
   try:
       return float(str(x).replace(',', '.'))
   except Exception:
       return None

def remove_accents(txt):
    return "".join(
        letter
        for letter in unicodedata.normalize("NFD", txt)
        if not unicodedata.combining(letter)
    )

def replace_nested_attribute_by_id(df, attribute_name, new_id_col, attribute_id_col='id'):
    df[new_id_col] = [attribute[attribute_id_col] for attribute in df[attribute_name]]
    df.drop(attribute_name, inplace=True, axis=1)
    return df

def main(args):
    download_file(args[0], args[1])

if __name__ == '__main__':
    main(sys.argv[1:])
