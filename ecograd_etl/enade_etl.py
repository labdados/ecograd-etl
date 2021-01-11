from utils import download_file
import sys

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

def download_enade(url, output_dir):
    download_file(url, output_dir)

def main(args):
    output_dir = "data"
    for url in enade_urls.values():
        download_enade(url, output_dir)

if __name__ == '__main__':
    main(sys.argv[1:])