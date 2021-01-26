from dotenv import load_dotenv
import utils
import os
import pandas as pd

load_dotenv()

def create_ies_table(db_con):
    # pega a entrada mais recente de nome e sigla da IES
    ies = (
        pd.read_sql("""SELECT DISTINCT ano, codigo_da_ies AS id_ies, nome_da_ies AS nome_ies,
                         sigla_da_ies AS sigla_ies FROM inep.indicadores
                         ORDER BY ano DESC""", db_con)
        .groupby(["id_ies"])
        [["id_ies", "nome_ies", "sigla_ies"]]
        .first()
    )
    ies.to_sql("ies", db_con, "inep", index=False,  if_exists="replace")

def create_municipio_table(db_con, sql_table="municipio", sql_schema="inep"):
    # pega a entrada mais frequente de nome e uf de municipio para cada codigo
    mun = (
        pd.read_sql("""SELECT DISTINCT codigo_do_municipio AS id_municipio,
                         UPPER(municipio_do_curso) AS nome_municipio, sigla_da_uf AS uf
                         FROM inep.indicadores""", db_con)
        .groupby(["id_municipio"])
        .agg(lambda x:x.value_counts().index[0])
    )
    mun.to_sql(sql_table, db_con, sql_schema, index=False,  if_exists="replace")

def etl_indicadores_dimensional():
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    db_con = utils.connect_db(db_url)
    create_ies_table(db_con)
    create_municipio_table(db_con)

etl_indicadores_dimensional()
