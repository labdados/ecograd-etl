from dotenv import load_dotenv
import utils
import os
import pandas as pd
import sys
from zipfile import ZipFile

load_dotenv()

indicadores_enade = {
    2019: {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/resultados/2019/Conceito_Enade_2019.csv"
    },
    2018: {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2019/resultados_conceito_enade_2018.csv"
    },
    2017: {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2018/resultados_conceito_enade_2017.csv"
    },
    2016: {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2017/resultado_enade2016_portal_06_09_2017.csv",

    },
    2015: "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2017/conceito_enade_2015_portal_atualizado_03_10_2017.csv",
    2014: "https://download.inep.gov.br/educacao_superior/enade/planilhas/2014/conceito_enade_2014_atualizado_em_04122017.csv",
    2013: "https://download.inep.gov.br/educacao_superior/enade/planilhas/2013/conceito_enade_2013.csv",
    2012: "https://download.inep.gov.br/educacao_superior/enade/planilhas/2012/conceito_enade_2012.csv",
    2011: "https://download.inep.gov.br/educacao_superior/enade/planilhas/2011/tabela_enade_cpc_2011_retificado_08_02_13.csv",
    2010: "https://download.inep.gov.br/educacao_superior/enade/planilhas/2010/tabela_enade_cpc_2010.csv"
}

def download_indicadores(url, output_file):
    print(f"Downloading {url} to {output_file}")
    utils.download_file(url, output_file)

def load_indicadores(csv_file, db_con, sql_table, sql_schema="public"):
    print(f"Loading {csv_file} to {sql_schema}.{sql_table}")
    #cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    df = pd.read_csv(csv_file, delimiter = ";", low_memory=False, encoding="latin1", decimal=",",
                     thousands=".")
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.columns = utils.clean_col_names(df.columns)
    print(df.columns)
    cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    new_cols = df.columns.difference(cur_cols)
    if not cur_cols.empty and not new_cols.empty:
        print(f"New columns found: {new_cols}")
        utils.add_db_table_columns(new_cols, "text", db_con, sql_table, sql_schema)
    df.to_sql(sql_table, db_con, sql_schema, index=False, if_exists='append')

def main(args):
    output_dir = "data"
    db_url = utils.build_db_url("postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
                                os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"),
                                os.getenv("POSTGRES_DB"))
    sql_table = "enade"
    sql_schema = "indicadores"
    db_con = utils.connect_db(db_url)
    utils.create_db_schema(db_con, sql_schema)
    utils.drop_db_table(db_con, sql_table, sql_schema)
    for year, url in ind_enade_urls.items():
       csv_file = os.path.join(output_dir, f"conceito_enade_{year}.csv")
       download_indicadores(url, csv_file)
       load_indicadores(csv_file, db_con, sql_table, sql_schema)

if __name__ == '__main__':
    main(sys.argv[1:])
