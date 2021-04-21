from dotenv import load_dotenv
from ecograd_etl import utils
import os
import pandas as pd
import sys

load_dotenv()

def extract_regiao(json='https://servicodados.ibge.gov.br/api/v1/localidades/regioes'):
    return pd.read_json(json)

def extract_uf(json='https://servicodados.ibge.gov.br/api/v1/localidades/estados'):
    return pd.read_json(json)

def extract_mesorregiao(json='https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes'):
    return pd.read_json(json)

def extract_microrregiao(json='https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes'):
    return pd.read_json(json)

def extract_municipio(json='https://servicodados.ibge.gov.br/api/v1/localidades/municipios'):
    return pd.read_json(json)

def replace_nested_attribute_by_id(df, attribute_name, new_id_col, attribute_id_col='id'):
    df[new_id_col] = [attribute[attribute_id_col] for attribute in df[attribute_name]]
    df.drop(attribute_name, inplace=True, axis=1)
    return df

def transform_uf(df):
    df = replace_nested_attribute_by_id(df, 'regiao', 'regiao_id')
    return df

def transform_mesorregiao(df):
    df = replace_nested_attribute_by_id(df, 'UF', 'uf_id')
    return df

def transform_microrregiao(df):
    df = replace_nested_attribute_by_id(df, 'mesorregiao', 'mesorregiao_id')
    return df

def transform_municipio(df):
    df.drop('regiao-imediata', inplace=True, axis=1)
    df = replace_nested_attribute_by_id(df, 'microrregiao', 'microrregiao_id')
    return df

def extract_populacao_uf(json='https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2020/variaveis/9324?localidades=N3'):
    return pd.read_json(json)

def extract_populacao_municipio(json='https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2020/variaveis/9324?localidades=N6'):
    return pd.read_json(json)

def transform_populacao(df, ano='2020'):
    series = df['resultados'][0][0]['series']
    data = {
        "id_localidade": [item['localidade']['id'] for item in series],
        "populacao": [item['serie'][ano] for item in series]
    }
    return pd.DataFrame(data)

def load_dataframe_to_db(df, db_con, db_table, db_schema, if_exists='replace'):
    df.to_sql(db_table, db_con, db_schema, index=False, if_exists=if_exists)

def etl_localidades(db_con, db_schema):
    regiao = extract_regiao()
    load_dataframe_to_db(regiao, db_con, 'regiao', db_schema)
    uf = extract_uf()
    estado = transform_uf(uf)
    load_dataframe_to_db(estado, db_con, 'uf', db_schema)
    mesorregiao = extract_mesorregiao()
    mesorregiao = transform_mesorregiao(mesorregiao)
    load_dataframe_to_db(mesorregiao, db_con, 'mesorregiao', db_schema)
    microrregiao = extract_microrregiao()
    microrregiao = transform_microrregiao(microrregiao)
    load_dataframe_to_db(microrregiao, db_con, 'microrregiao', db_schema)
    municipio = extract_municipio()
    municipio = transform_municipio(municipio)
    load_dataframe_to_db(municipio, db_con, 'municipio', db_schema)

def etl_populacao_uf(db_con, db_schema):
    pop_uf = extract_populacao_uf()
    pop_uf = transform_populacao(pop_uf)
    load_dataframe_to_db(pop_uf, db_con, 'populacao', db_schema)
    pop_mun = extract_populacao_municipio()
    pop_mun = transform_populacao(pop_mun)
    load_dataframe_to_db(pop_mun, db_con, 'populacao', db_schema, if_exists='append')

def main(args):
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    db_engine = utils.create_db_engine(db_url)
    db_con = db_engine.connect()
    json = 'http://api.sidra.ibge.gov.br/values/t/6579/v/all/p/2020/N3/all?formato=json'
    data_dir = 'data'
    csv_out = os.path.join(data_dir, 'populacao_uf.csv')
    db_table = 'populacao_uf'
    db_schema = 'ibge'
    utils.create_db_schema(db_con, db_schema)
    etl_populacao_uf(db_con, db_schema)
    etl_localidades(db_con, db_schema)

if __name__ == '__main__':
    main(sys.argv[1:])
