from dotenv import load_dotenv
from ecograd_etl import utils
import os
import pandas as pd

load_dotenv()

def create_area_table(db_con, sql_table="area", sql_schema="inep"):
    # pega a entrada mais frequente de nome de area para cada codigo
    df = (
        pd.read_sql("""SELECT DISTINCT codigo_da_area AS id_area,
                        UPPER(area_de_avaliacao) AS nome_area
                        FROM inep.cpc""", db_con)
        .drop_duplicates(subset=["id_area"], keep="first")
    )
    df.to_sql(sql_table, db_con, sql_schema, index=False,  if_exists="replace")

def create_categoria_admin_table(db_con, sql_table="categoria_administrativa", sql_schema="inep"):
    # pega a entrada mais recente de nome e sigla da IES
    df = (
        pd.read_sql("""
        SELECT ROW_NUMBER() OVER (ORDER BY categoria_administrativa) AS id_categoria_administrativa,
        categoria_administrativa
        FROM (SELECT DISTINCT categoria_administrativa FROM inep.cpc) t1
                    """, db_con)
    )
    df.to_sql(sql_table, db_con, sql_schema, index=False,  if_exists="replace")

def create_ies_table(db_con, sql_table="ies", sql_schema="inep"):
    # pega a entrada mais recente de nome e sigla da IES
    df = (
        pd.read_sql("""SELECT DISTINCT codigo_da_ies AS id_ies, nome_da_ies AS nome_ies,
                         sigla_da_ies AS sigla_ies FROM inep.cpc
                    """, db_con)
        .drop_duplicates(subset=["id_ies"], keep="first")
    )
    df.to_sql(sql_table, db_con, sql_schema, index=False,  if_exists="replace")

def create_municipio_table(db_con, sql_table="municipio", sql_schema="inep"):
    # pega a entrada mais frequente de nome e uf de municipio para cada codigo
    df = (
        pd.read_sql("""SELECT DISTINCT codigo_do_municipio AS id_municipio,
                         UPPER(municipio_do_curso) AS nome_municipio, sigla_da_uf AS uf
                         FROM inep.cpc""", db_con)
        .groupby(["id_municipio"])
        .agg(lambda x:x.value_counts().index[0])
        .reset_index()
    )
    df.to_sql(sql_table, db_con, sql_schema, index=False,  if_exists="replace")

def create_fact_table(db_con, sql_table="avaliacao", sql_schema="inep"):
    df = (
        pd.read_sql("""
            SELECT cpc.ano, cpc.codigo_do_curso AS id_curso, cpc.codigo_da_area AS id_area,
                   cpc.codigo_da_ies AS id_ies, ca.id_categoria_administrativa,
                   cpc.codigo_do_municipio AS id_municipio, cpc.cpc_continuo, cpc.cpc_faixa,
	               enade.conceito_enade_continuo, enade.conceito_enade_faixa
	        FROM inep.cpc cpc
	        LEFT OUTER JOIN inep.enade enade
                ON (cpc.ano = enade.ano and cpc.codigo_do_curso = enade.codigo_do_curso)
	        LEFT OUTER JOIN inep.categoria_administrativa AS ca
                ON cpc.categoria_administrativa = ca.categoria_administrativa""", db_con)
    )
    df.to_sql(sql_table, db_con, sql_schema, index=False,  if_exists="replace")

def etl_indicadores_dimensional():
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    db_con = utils.connect_db(db_url)
    create_ies_table(db_con)
    create_municipio_table(db_con)
    create_area_table(db_con)
    create_categoria_admin_table(db_con)
    create_fact_table(db_con)

etl_indicadores_dimensional()
