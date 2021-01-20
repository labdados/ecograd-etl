from dotenv import load_dotenv
import utils
import os
import pandas as pd
import sqlalchemy
import sys
from zipfile import ZipFile

load_dotenv()

ind_enade = {
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
    2015: {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2017/conceito_enade_2015_portal_atualizado_03_10_2017.csv"
    },
    2014: {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2014/conceito_enade_2014_atualizado_em_04122017.csv"
    },
    2013: {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2013/conceito_enade_2013.csv"
    },
    2012: {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2012/conceito_enade_2012.csv"
    },
    2011: {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2011/tabela_enade_cpc_2011_retificado_08_02_13.csv",
    },
    2010: {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2010/tabela_enade_cpc_2010.csv"
    }
}

ind_rename_columns = {
    "AREA": "AREA_DE_AVALIACAO",
    "AREA_DE_ENQUADRAMENTO": "AREA_DE_AVALIACAO",
    "CATEG_ADMINISTRATIVA": "CATEGORIA_ADMINISTRATIVA",
    "COD_AREA": "CODIGO_DA_AREA",
    "COD_CURSOS_DA_UNIDADE": "CODIGO_DO_CURSO",
    "COD_IES": "CODIGO_DA_IES",
    "COD_MUNICIPIO": "CODIGO_DO_MUNICIPIO",
    "CONCEITO_ENADE": "CONCEITO_ENADE_FAIXA",
    "CONCLUINTES_INSCRITOS": "N_DE_CONCLUINTES_INSCRITOS",
    "CONCLUINTES_PARTICIPANTES": "N_DE_CONCLUINTES_PARTICIPANTES",
    "CONLUINTES_INSCRITOS": "N_DE_CONCLUINTES_INSCRITOS",
    "INSCRITOS": "N_DE_CONCLUINTES_INSCRITOS",
    "NOME_DO_MUNICIPIO": "MUNICIPIO_DO_CURSO",
    "NOTA_CONTINUA_DO_ENADE": "CONCEITO_ENADE_CONTINUO",
    "OBS": "OBSERVACAO",
    "ORG_ACADEMICA": "ORGANIZACAO_ACADEMICA",
    "PARTICIPANTES": "N_DE_CONCLUINTES_PARTICIPANTES",
    "UF_DO_CURSO": "SIGLA_DA_UF"
}

ind_na_values = [
    "", "-", ".",
    "Resultado desconsiderado devido à Política de Transferência Assistida (Portaria MEC nº 24/2016)"
]

sql_dtypes = {
    "CODIGO_DO_CURSO": sqlalchemy.types.Text
}

def download_indicadores(url, output_file):
    print(f"Downloading {url} to {output_file}")
    utils.download_file(url, output_file)

def load_indicadores(csv_file, db_con, sql_table, sql_schema="public", cols_to_rename=None):
    print(f"Loading {csv_file} to {sql_schema}.{sql_table}")
    df = pd.read_csv(csv_file, delimiter = ";", low_memory=False, encoding="latin1", decimal=",",
                     na_values=ind_na_values)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.columns = utils.clean_col_names(df.columns)
    if cols_to_rename:
        print(f"Renaming columns: {cols_to_rename}")
        df.rename(columns=cols_to_rename, inplace=True)
    print(df.columns)
    cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    new_cols = df.columns.difference(cur_cols)
    if not cur_cols.empty and not new_cols.empty:
        print(f"New columns found: {new_cols}")
        utils.add_db_table_columns(new_cols, "text", db_con, sql_table, sql_schema)
    df.to_sql(sql_table, db_con, sql_schema, index=False, if_exists='append',
              dtype=sql_dtypes)

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
    for year, item in ind_enade.items():
       csv_file = os.path.join(output_dir, f"conceito_enade_{year}.csv")
       download_indicadores(item["url"], csv_file)
       #cols_to_rename = item["rename_columns"] if "rename_columns" in item else None
       load_indicadores(csv_file, db_con, sql_table, sql_schema, ind_rename_columns)

if __name__ == '__main__':
    main(sys.argv[1:])
