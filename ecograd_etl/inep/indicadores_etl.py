from dotenv import load_dotenv
from ecograd_etl import utils
from ecograd_etl.inep import config
import os
import pandas as pd
import sys

load_dotenv()

def setup_db(db_url, sql_table, sql_schema):
    db_con = utils.connect_db(db_url)
    utils.create_db_schema(db_con, sql_schema)
    utils.drop_db_table(db_con, sql_table, sql_schema)
    return db_con

def extract_indicadores(csv_file, na_values={}, **kwargs):
    return pd.read_csv(csv_file, low_memory=False, delimiter = ";",
                       encoding="latin1", decimal=",", thousands=".",
                       na_values=na_values, **kwargs)

def transform_indicadores(df, year, rename_cols={}, replace_values={}, converters={}):
    df.dropna(axis = 0, how = "all", inplace=True) # drop empty rows
    df.dropna(axis = 1, how = "all", inplace=True) # drop empty columns
    df.columns = utils.clean_col_names(df.columns)
    cols_to_rename = utils.filter_dict_by_keys(rename_cols, df.columns)
    if cols_to_rename:
        print(f"Renaming columns: {cols_to_rename}")
        df.rename(columns=cols_to_rename, inplace=True)
    if 'ano' not in df.columns:
        df['ano'] = year
    df.replace(replace_values, inplace=True)
    cols_to_convert = utils.filter_dict_by_keys(converters, df.columns)
    for col, fn in cols_to_convert.items():
        print(f"Applying {fn} to {col}")
        df[col] = df[col].apply(fn)
    return df

def load_indicadores(df, csv_file, db_con, sql_table, sql_schema, sql_dtype={}):
    print(f"Loading {csv_file} to {sql_schema}.{sql_table}")
    cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    new_cols = df.columns.difference(cur_cols)
    if not cur_cols.empty and not new_cols.empty:
        print(f"New columns found: {new_cols}")
        utils.add_db_table_columns(new_cols, "text", db_con, sql_table, sql_schema)
        missing_cols = cur_cols.difference(df.columns)
        print(f"Missing columns: {missing_cols}")
    df.to_sql(sql_table, db_con, sql_schema, index=False, if_exists='append',
              dtype=sql_dtype)

def etl_indicadores(years, db_url, conf, dataset):
    data_dir = conf['data_dir']
    sql_schema = conf['sql_schema']
    sql_table = conf['datasets'][dataset]['sql_table']
    db_con = setup_db(db_url, sql_table, sql_schema)
    dataset_items = conf['datasets'][dataset]['items']
    for year in years:
        item_conf = dataset_items[year]
        csv_file = os.path.join(data_dir, f"inep_{dataset}_{year}.csv")
        print(f"Downloading file {csv_file}")
        utils.download_file(item_conf['url'], csv_file)
        extract_kwargs = item_conf['extract_kwargs'] if 'extract_kwargs' in item_conf else {}
        df = extract_indicadores(csv_file, conf['na_values'], **extract_kwargs)
        rename_coluns = conf['rename_columns'] if 'rename_columns' in conf else {}
        replace_values = conf['replace_values'] if 'replace_values' in conf else {}
        converters = conf['converters'] if 'converters' in conf else {}
        df = transform_indicadores(df, year, rename_coluns, replace_values, converters)
        print("Columns: ", df.columns)
        sql_dtype = conf['dtype'] if 'dtype' in conf else {}
        load_indicadores(df, csv_file, db_con, sql_table, sql_schema, sql_dtype)

def main(args):
    conf = config.conf
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    #datasets = conf['datasets'].keys()
    datasets = ['idd']
    for dataset in datasets:
        years = conf['datasets'][dataset]['items'].keys() if len(args) == 0 else args
        etl_indicadores(years, db_url, conf, dataset)

if __name__ == '__main__':
    main(sys.argv[1:])
