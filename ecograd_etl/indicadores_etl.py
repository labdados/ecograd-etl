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
    }#,
    # "2012": {
    #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2012/conceito_enade_2012.csv",
    #     "extract_kwargs": {
    #         "skiprows": 1
    #     }
    # },
    # "2011": {
    #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2011/tabela_enade_cpc_2011_retificado_08_02_13.csv",
    # },
    # "2010": {
    #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2010/tabela_enade_cpc_2010.csv"
    # }
}

rename_columns = {
    "ano_enade": "ano",
    "area": "area_de_avaliacao",
    "area_de_enquadramento": "area_de_avaliacao",
    "area_enquadramento": "area_de_avaliacao",
    "categ_administrativa": "categoria_administrativa",
    "cod_area": "codigo_da_area",
    "cod_cursos_da_unidade": "codigo_do_curso",
    "cod_ies": "codigo_da_ies",
    "cod_municipio": "codigo_do_municipio",
    "codigo_area": "codigo_da_area",
    "codigo_ies": "codigo_da_ies",
    "codigo_municipio": "codigo_do_municipio",
    "codigo_uf": "sigla_da_uf",
    "conceito_enade": "conceito_enade_faixa",
    "concluintes_inscritos": "n_de_concluintes_inscritos",
    "concluintes_participantes": "n_de_concluintes_participantes",
    "conluintes_inscritos": "n_de_concluintes_inscritos",
    "ies": "nome_da_ies",
    "inscritos": "n_de_concluintes_inscritos",
    "nome_do_municipio": "municipio_do_curso",
    "nota_continua_do_enade": "conceito_enade_continuo",
    "obs": "observacao",
    "org_academica": "organizacao_academica",
    "participantes": "n_de_concluintes_participantes",
    "sigla_ies": "sigla_da_ies",
    "sigla_uf": "sigla_da_uf",
    "uf_do_curso": "sigla_da_uf"
}

na_values = [
    "", "-", ".",
    "Resultado desconsiderado devido à Política de Transferência Assistida (Portaria MEC nº 24/2016)"
]

sql_dtypes = {
    "codigo_do_curso": sqlalchemy.types.Text
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
    df.dropna(axis = 0, how = "all", inplace=True)
    df.dropna(axis = 1, how = "all", inplace=True)
    #df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
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
