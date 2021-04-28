from dotenv import load_dotenv
from ecograd_etl import utils
from ecograd_etl.inep import censo_config
import os
import pandas as pd
import sys

load_dotenv()

def extract_ies(input_file, delimiter='|', na_values={}, **kwargs):
    df = pd.read_csv(input_file, low_memory=False, delimiter=delimiter,
                     encoding="latin1", decimal=",", thousands=".",
                     na_values=na_values, **kwargs)
    return df

def etl_censo(years, conf, dataset):
    data_dir = conf['data_dir']
    dataset_items = conf['datasets'][dataset]['items']
    for year in years:
        item_conf = dataset_items[year]
        zip_file_name = os.path.join(data_dir, item_conf['filename'])
        extract_kwargs = item_conf['extract_kwargs'] if 'extract_kwargs' in item_conf else {}
        na_values = conf['na_values'] if 'na_values' in conf else {}
        input_file = utils.open_file_from_zip(zip_file_name, extension='csv', prefix='SUP_IES')
        df = extract_ies(input_file, '|', na_values, **extract_kwargs)
        print(df)

def main(args):
    conf = censo_config.conf
    datasets = conf['datasets'].keys()
    for dataset in datasets:
        years = conf['datasets'][dataset]['items'].keys() if len(args) == 0 else args
        etl_censo(years, conf, dataset)

if __name__ == '__main__':
    main(sys.argv[1:])
