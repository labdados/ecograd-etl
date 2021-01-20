from dotenv import load_dotenv
import utils
import os
import pandas as pd
import sqlalchemy
import sys
from zipfile import ZipFile

load_dotenv()

enade_conf = {
    "2019": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/resultados/2019/Conceito_Enade_2019.csv"
    },
    "2018": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2019/resultados_conceito_enade_2018.csv"
    },
    "2017": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2018/resultados_conceito_enade_2017.csv"
    },
    "2016": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2017/resultado_enade2016_portal_06_09_2017.csv",
    },
    "2015": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2017/conceito_enade_2015_portal_atualizado_03_10_2017.csv"
    },
    "2014": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2014/conceito_enade_2014_atualizado_em_04122017.csv"
    },
    "2013": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2013/conceito_enade_2013.csv"
    },
    "2012": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2012/conceito_enade_2012.csv",
        "extract_kwargs": {
            "skiprows": 1
        }
    },
    "2011": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2011/tabela_enade_cpc_2011_retificado_08_02_13.csv",
    },
    "2010": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2010/tabela_enade_cpc_2010.csv"
    }
}

rename_columns = {
    "ANO_ENADE": "ANO",
    "AREA": "AREA_DE_AVALIACAO",
    "AREA_DE_ENQUADRAMENTO": "AREA_DE_AVALIACAO",
    "AREA_ENQUADRAMENTO": "AREA_DE_AVALIACAO",
    "CATEG_ADMINISTRATIVA": "CATEGORIA_ADMINISTRATIVA",
    "COD_AREA": "CODIGO_DA_AREA",
    "COD_CURSOS_DA_UNIDADE": "CODIGO_DO_CURSO",
    "COD_IES": "CODIGO_DA_IES",
    "COD_MUNICIPIO": "CODIGO_DO_MUNICIPIO",
    "CODIGO_AREA": "CODIGO_DA_AREA",
    "CODIGO_IES": "CODIGO_DA_IES",
    "CODIGO_MUNICIPIO": "CODIGO_DO_MUNICIPIO",
    "CODIGO_UF": "SIGLA_DA_UF",
    "CONCEITO_ENADE": "CONCEITO_ENADE_FAIXA",
    "CONCLUINTES_INSCRITOS": "N_DE_CONCLUINTES_INSCRITOS",
    "CONCLUINTES_PARTICIPANTES": "N_DE_CONCLUINTES_PARTICIPANTES",
    "CONLUINTES_INSCRITOS": "N_DE_CONCLUINTES_INSCRITOS",
    "IES": "NOME_DA_IES",
    "INSCRITOS": "N_DE_CONCLUINTES_INSCRITOS",
    "NOME_DO_MUNICIPIO": "MUNICIPIO_DO_CURSO",
    "NOTA_CONTINUA_DO_ENADE": "CONCEITO_ENADE_CONTINUO",
    "OBS": "OBSERVACAO",
    "ORG_ACADEMICA": "ORGANIZACAO_ACADEMICA",
    "PARTICIPANTES": "N_DE_CONCLUINTES_PARTICIPANTES",
    "SIGLA_IES": "SIGLA_DA_IES",
    "SIGLA_UF": "SIGLA_DA_UF",
    "UF_DO_CURSO": "SIGLA_DA_UF"
}

na_values = [
    "", "-", ".",
    "Resultado desconsiderado devido à Política de Transferência Assistida (Portaria MEC nº 24/2016)"
]

sql_dtypes = {
    "CODIGO_DO_CURSO": sqlalchemy.types.Text
}

def setup_db(db_url, sql_table, sql_schema):
    db_con = utils.connect_db(db_url)
    utils.create_db_schema(db_con, sql_schema)
    utils.drop_db_table(db_con, sql_table, sql_schema)
    return db_con

def download_indicadores(url, output_file):
    print(f"Downloading {url} to {output_file}")
    utils.download_file(url, output_file)

def extract_indicadores(csv_file, **kwargs):
    return pd.read_csv(csv_file, delimiter = ";", low_memory=False,
                       encoding="latin1", decimal=",", na_values=na_values,
                       **kwargs)

def transform_indicadores(df, year):
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.dropna(how = "all", inplace=True)
    df.columns = utils.clean_col_names(df.columns)
    cols_to_rename = utils.filter_dict_by_keys(rename_columns, df.columns)
    if cols_to_rename:
        print(f"Renaming columns: {cols_to_rename}")
        df.rename(columns=cols_to_rename, inplace=True)
    if 'ano' not in df.columns:
        df['ano'] = year
    return df

def load_indicadores(df, csv_file, db_con, sql_table, sql_schema):
    print(f"Loading {csv_file} to {sql_schema}.{sql_table}")
    cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    new_cols = df.columns.difference(cur_cols)
    if not cur_cols.empty and not new_cols.empty:
        print(f"New columns found: {new_cols}")
        utils.add_db_table_columns(new_cols, "text", db_con, sql_table, sql_schema)
    df.to_sql(sql_table, db_con, sql_schema, index=False, if_exists='append',
              dtype=sql_dtypes)

def etl_indicadores(years, db_url, sql_table, sql_schema, data_dir="data"):
    db_con = setup_db(db_url, sql_table, sql_schema)
    for year in years:
        conf = enade_conf[year]
        csv_file = os.path.join(data_dir, f"conceito_enade_{year}.csv")
        download_indicadores(conf["url"], csv_file)
        extract_kwargs = conf["extract_kwargs"] if "extract_kwargs" in conf else {}
        df = extract_indicadores(csv_file, **extract_kwargs)
        df = transform_indicadores(df, year)
        print(df.columns)
        load_indicadores(df, csv_file, db_con, sql_table, sql_schema)

def main(args):
    years = enade_conf.keys() if len(args) == 0 else args
    data_dir = "data"
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    sql_table = "enade"
    sql_schema = "indicadores"
    etl_indicadores(years, db_url, sql_table, sql_schema, data_dir)

if __name__ == '__main__':
    main(sys.argv[1:])
