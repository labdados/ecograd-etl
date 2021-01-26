from dotenv import load_dotenv
import ecograd_etl.utils as utils
import os
import pandas as pd

load_dotenv()

def create_ies_table(db_con):
    ies = pd.read_sql("SELECT DISTINCT ano, codigo_da_ies, nome_da_ies, sigla_da_ies FROM inep.indicadores", db_con)
    ies = ies.sort_values(by=["ano"], ascending=False).groupby(["codigo_da_ies"])["codigo_da_ies", "nome_da_ies", "sigla_da_ies"].first()
    ies.to_sql("ies", db_con, "inep", index=False,  if_exists="replace")

def etl_indicadores_dimensional():
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    db_con = utils.connect_db(db_url)
    create_ies_table(db_con)