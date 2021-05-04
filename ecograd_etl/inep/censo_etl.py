from dotenv import load_dotenv
from ecograd_etl import utils
from ecograd_etl.inep import censo_config
import os
import pandas as pd
import sys

load_dotenv()

def extract_censo(input_file, delimiter='|', na_values={}, **kwargs):
    df = pd.read_csv(input_file, low_memory=False, delimiter=delimiter,
                     encoding="latin1", decimal=",", thousands=".",
                     na_values=na_values, **kwargs)
    return df

def transform_censo(df, year, rename_cols={}, duplicate_cols={},
                          replace_values={}, converters={}):
    # Drop empty rows and columns
    df.dropna(axis = 0, how = "all", inplace=True)
    df.dropna(axis = 1, how = "all", inplace=True)
    # Clean column names
    df.columns = utils.clean_col_names(df.columns)
    # Rename columns
    cols_to_rename = utils.filter_dict_by_keys(rename_cols, df.columns)
    if cols_to_rename:
        print(f"Renaming columns: {cols_to_rename}")
        df.rename(columns=cols_to_rename, inplace=True)
    # Duplicate col to new_col. The idea is that one can change values after duplicating
    for new_col, col in duplicate_cols.items():
        df[new_col] = df[col]
    # Replace values
    df.replace(replace_values, inplace=True)
    cols_to_convert = utils.filter_dict_by_keys(converters, df.columns)
    # Apply function to columns
    for col, fn in cols_to_convert.items():
        print(f"Applying {fn} to {col}")
        df[col] = df[col].apply(fn)
    # Add ano column if missing
    if 'ano' not in df.columns:
        df['ano'] = year
    return df

def load_censo(df, db_con, sql_table, sql_schema, sql_dtype={}):
    print(f"Loading {sql_schema}.{sql_table}")
    cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    new_cols = df.columns.difference(cur_cols)
    if not cur_cols.empty and not new_cols.empty:
        print(f"New columns found: {new_cols}")
        utils.add_db_table_columns(new_cols, "text", db_con, sql_table, sql_schema)
        missing_cols = cur_cols.difference(df.columns)
        print(f"Missing columns: {missing_cols}")
    df.to_sql(sql_table, db_con, sql_schema, index=False, if_exists='append',
              dtype=sql_dtype)

def etl_censo(years, db_con, conf, dataset):
    data_dir = conf['data_dir']
    sql_schema = conf['sql_schema']
    #sql_table = conf['datasets'][dataset]['sql_table']
    dataset_items = conf['datasets'][dataset]['items']
    tables = conf['datasets'][dataset]['tables']
    for table in tables.values():
        utils.clean_db(db_con, table['table_name'], sql_schema)
    for year in years:
        item_conf = dataset_items[year]
        zip_file_name = os.path.join(data_dir, item_conf['filename'])
        extract_kwargs = item_conf['extract_kwargs'] if 'extract_kwargs' in item_conf else {}
        na_values = conf['na_values'] if 'na_values' in conf else {}
        for table in tables.values():
            table_name = table['table_name']
            file_extension = table['file_extension']
            file_regex = table['file_regex']
            print(f"Opening file inside {file_regex}.{file_extension} in {zip_file_name}")
            input_file = utils.open_file_from_zip(zip_file_name, extension=file_extension, regex=file_regex)
            df = extract_censo(input_file, '|', na_values, **extract_kwargs)
            rename_cols = conf['rename_columns'] if 'rename_columns' in conf else {}
            duplicate_cols = conf['duplicate_columns'] if 'duplicate_columns' in conf else {}
            replace_values = conf['replace_values'] if 'replace_values' in conf else {}
            converters = conf['converters'] if 'converters' in conf else {}
            df = transform_censo(df, year, rename_cols, duplicate_cols, replace_values, converters)
            print("Columns: ", df.columns)
            sql_dtype = conf['dtype'] if 'dtype' in conf else {}
            load_censo(df, db_con, table_name, sql_schema, sql_dtype)

def main(args):
    conf = censo_config.conf
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    db_engine = utils.create_db_engine(db_url)
    datasets = conf['datasets'].keys()
    for dataset in datasets:
        years = conf['datasets'][dataset]['items'].keys() if len(args) == 0 else args
        db_con = db_engine.connect()
        etl_censo(years, db_con, conf, dataset)

if __name__ == '__main__':
    main(sys.argv[1:])
