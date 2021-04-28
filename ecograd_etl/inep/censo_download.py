from dotenv import load_dotenv
from ecograd_etl import utils
from ecograd_etl.inep import censo_config
import os
import sys

load_dotenv()

def download_censo(conf, dataset, years):
    data_dir = conf['data_dir']
    dataset_items = conf['datasets'][dataset]['items']
    for year in years:
        item_conf = dataset_items[year]
        file_url = item_conf['url']
        file_extension = utils.get_file_extension(file_url)
        file_name = os.path.join(data_dir, item_conf['filename'])
        print(f"Downloading file {file_name}")
        utils.download_file(file_url, file_name)

def main(args):
    conf = censo_config.conf
    datasets = conf['datasets'].keys()
    for dataset in datasets:
        years = conf['datasets'][dataset]['items'].keys() if len(args) == 0 else args
        download_censo(conf, dataset, years)

if __name__ == '__main__':
    main(sys.argv[1:])
