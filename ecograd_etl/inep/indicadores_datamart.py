from dotenv import load_dotenv
from ecograd_etl import utils
from ecograd_etl.inep import config
import os
import pandas as pd
import sys

load_dotenv()

def create_ano_table(db_con, table_name, source_schema, datamart_schema):
    # pega a entrada mais frequente de nome de area para cada codigo
    df = (
        pd.read_sql(f"""SELECT DISTINCT ano
                        FROM {source_schema}.cpc
                        ORDER BY ano""", db_con)
    )
    df.index += 1 # id starting from 1
    df.to_sql(table_name, db_con, datamart_schema, index_label='id', if_exists='replace')

def create_area_de_avaliacao_table(db_con, table_name, source_schema, datamart_schema):
    # pega a entrada mais frequente de nome de area para cada codigo
    df = (
        pd.read_sql(f"""SELECT DISTINCT codigo_da_area AS cod_area,
                        UPPER(area_de_avaliacao) AS nome_area
                        FROM {source_schema}.cpc
                        ORDER BY cod_area""", db_con)
        .drop_duplicates(subset=['cod_area'], keep='first')
    )
    df.index += 1 # id starting from 1
    df.to_sql(table_name, db_con, datamart_schema, index_label='id', if_exists='replace')

def create_categoria_admin_table(db_con, table_name, source_schema, datamart_schema):
    # pega a entrada mais recente de nome e sigla da IES
    df = (
        pd.read_sql(f"""
        SELECT DISTINCT categoria_administrativa
        FROM {source_schema}.cpc
        ORDER BY categoria_administrativa""", db_con)
    )
    df.index += 1 # id starting from 1
    df.to_sql(table_name, db_con, datamart_schema, index_label='id', if_exists='replace')

def create_curso_table(db_con, table_name, source_schema, datamart_schema):
    # pega a entrada mais frequente de nome de area para cada codigo
    df = (
        pd.read_sql(f"""SELECT DISTINCT codigo_do_curso AS cod_curso, modalidade_de_ensino
                        FROM {source_schema}.cpc
                        ORDER BY cod_curso""", db_con)
    )
    df.index += 1 # id starting from 1
    df.to_sql(table_name, db_con, datamart_schema, index_label='id', if_exists='replace')

def create_ies_table(db_con, table_name, source_schema, datamart_schema):
    # pega a entrada mais recente de nome e sigla da IES
    df = (
        pd.read_sql(f"""SELECT DISTINCT codigo_da_ies AS cod_ies, nome_da_ies AS nome_ies,
                         sigla_da_ies AS sigla_ies
                         FROM {source_schema}.cpc
                         ORDER BY cod_ies
                    """, db_con)
        .drop_duplicates(subset=["cod_ies"], keep="first")
    )
    df.index += 1 # id starting from 1
    df.to_sql(table_name, db_con, datamart_schema, index_label='id', if_exists='replace')

def create_municipio_table(db_con, table_name, source_schema, datamart_schema):
    # pega a entrada mais frequente de nome e uf de municipio para cada codigo
    df = (
        pd.read_sql(f"""SELECT DISTINCT codigo_do_municipio AS cod_municipio,
                         UPPER(municipio_do_curso) AS nome_municipio, sigla_da_uf AS uf
                         FROM {source_schema}.cpc
                         ORDER BY cod_municipio""", db_con)
        .groupby(['cod_municipio'])
        .agg(lambda x:x.value_counts().index[0])
        .reset_index()
    )
    df.index += 1 # id starting from 1
    df.to_sql(table_name, db_con, datamart_schema, index_label='id', if_exists='replace')

def create_fact_table(db_con, table_name, source_schema, datamart_schema):
    df = (
        pd.read_sql(f"""
            SELECT cpc.ano, cpc.codigo_do_curso AS id_curso, cpc.codigo_da_area AS id_area,
                   cpc.codigo_da_ies AS id_ies, ca.id_categoria_administrativa,
                   cpc.codigo_do_municipio AS id_municipio, cpc.cpc_continuo, cpc.cpc_faixa,
	               enade.conceito_enade_continuo, enade.conceito_enade_faixa,
                   idd.idd_continuo, idd.idd_faixa
	        FROM {source_schema}.cpc cpc
	        LEFT OUTER JOIN {source_schema}.enade enade
                ON (cpc.ano = enade.ano and cpc.codigo_do_curso = enade.codigo_do_curso)
            LEFT OUTER JOIN {source_schema}.idd idd
                ON (cpc.ano = idd.ano and cpc.codigo_do_curso = idd.codigo_do_curso)
	        LEFT OUTER JOIN {source_schema}.categoria_administrativa AS ca
                ON cpc.categoria_administrativa = ca.categoria_administrativa""", db_con)
    )
    df.to_sql(table_name, db_con, datamart_schema, index=False, if_exists='replace')

def etl_indicadores_dimensional(db_con, conf):
    source_schema = conf['sql_schema']
    datamart_schema = source_schema + '_datamart'
    utils.create_db_schema(db_con, datamart_schema)
    print("Creating dm_ano table")
    create_ano_table(db_con, 'dm_ano', source_schema, datamart_schema)
    print("Creating dm_curso table")
    create_curso_table(db_con, 'dm_curso', source_schema, datamart_schema)
    print("Creating dm_ies table")
    create_ies_table(db_con, 'dm_ies', source_schema, datamart_schema)
    print("Creating dm_municipio table")
    create_municipio_table(db_con, 'dm_municipio', source_schema, datamart_schema)
    print("Creating dm_area_de_avaliacao table")
    create_area_de_avaliacao_table(db_con, 'dm_area_de_avaliacao', source_schema, datamart_schema)
    print("Creating dm_categoria_administrativa table")
    create_categoria_admin_table(db_con, 'dm_categoria_administrativa', source_schema, datamart_schema)
    print("Creating ft_indicadores table")
    create_fact_table(db_con, 'ft_indicadores', source_schema, datamart_schema)

def main(args):
    conf = config.conf
    db_url = utils.build_db_url(
        'postgresql', os.getenv('POSTGRES_USER'), os.getenv('POSTGRES_PWD'),
        os.getenv('POSTGRES_HOST'), os.getenv('POSTGRES_PORT'), os.getenv('POSTGRES_DB')
    )
    db_engine = utils.create_db_engine(db_url)
    db_con = db_engine.connect()
    etl_indicadores_dimensional(db_con, conf)

if __name__ == '__main__':
    main(sys.argv[1:])
