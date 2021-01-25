from dotenv import load_dotenv
import utils
import os
import pandas as pd
import sqlalchemy
import sys
from zipfile import ZipFile

load_dotenv()

cpc_conf = {
    "2019": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/cpc/2019/resultados_cpc_2019.csv"
    },
    "2018": {
        "url": "https://download.inep.gov.br/educacao_superior/igc_cpc/2018/portal_CPC_edicao2018.csv"
    },
    "2017": {
        "url": "https://download.inep.gov.br/educacao_superior/igc_cpc/2018/resultado_cpc_2017.csv"
    },
    "2016": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2017/Resultado_CPC_2016_portal_23_02_2018.csv"
    },
    "2015": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/legislacao/2017/cpc_2015_portal_atualizado_03_10_2017.csv"
    },
    "2014": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2014/cpc2014_atualizado_em_04122017.csv"
    },
    "2013": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2013/cpc2013_atualizado_em_27112017.csv"        
    },
    "2012": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2012/cpc_2012_site_2014_03_14.csv"
    },
    "2011": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2011/tabela_enade_cpc_2011_retificado_08_02_13.csv"
    },
    "2010": {
        "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2010/tabela_enade_cpc_2010.csv"
    }
}

enade_conf = {
    "2019": {
        "url": "https://download.inep.gov.br/educacao_superior/indicadores/resultados/2019/Conceito_Enade_2019.csv",
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

rename_columns_enade = {
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

rename_columns_cpc = {
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
    "de_doutores": "nota_bruta_doutores",
    "de_mestres": "nota_bruta_mestres",
    "edicao": "ano",
    "ies": "nome_da_ies",
    "inscritos": "n_de_concluintes_inscritos",
    "nome_do_municipio": "municipio_do_curso",
    "n_de_concluintes_participantes_com_nota_no_enem": "concluintes_participantes_com_nota_no_enem",
    "nota_bruta_do_idd": "nota_bruta_idd",
    "nota_bruta_infraestrutura": "nota_bruta_infraestrutura_e_instalacoes_fisicas",
    "nota_bruta_oport_ampliacao": "nota_bruta_oportunidade_de_ampliacao_da_formacao",
    "nota_bruta_oportunidades_de_ampliacao_da_formacao": "nota_bruta_oportunidade_de_ampliacao_da_formacao",
    "nota_bruta_org_didatico_pedagogica": "nota_padronizada_organizacao_didatico_pedagogica",
    "nota_continua_do_enade": "conceito_enade_continuo",
    "nota_padronizada_de_doutores": "nota_padronizada_doutores",
    "nota_padronizada_de_mestres": "nota_padronizada_mestres",
    "nota_padronizada_de_regime_de_trabalho": "nota_padronizada_regime_de_trabalho",
    "nota_padronizada_idd": "nota_padronizada_do_idd",
    "nota_padronizada_infraestrutura": "nota_padronizada_infraestrutura_e_instalacoes_fisicas",
    "nota_padronizada_oportunidades_de_ampliacao_da_formacao": "nota_padronizada_oportunidade_de_ampliacao_da_formacao",
    "nota_padronizada_org_didatico_pedagogica": "nota_padronizada_organizacao_didatico_pedagogica",
    "nr_cursos_da_unidade": "numero_de_cursos_da_unidade",
    "nr_de_docentes": "n_de_docentes",
    "obs": "observacao",
    "org_academica": "organizacao_academica",
    "participantes": "n_de_concluintes_participantes",
    "percentual_de_concluintes_participantes_com_nota_no_enem": "proporcao_de_concluintes_participantes_com_nota_no_enem",
    "sigla_ies": "sigla_da_ies",
    "sigla_uf": "sigla_da_uf",
    "uf_do_curso": "sigla_da_uf"
}

na_values_enade = [
    "", "-", ".",
    "Resultado desconsiderado devido à Política de Transferência Assistida (Portaria MEC nº 24/2016)"
]

na_values_cpc = ["", "-", "."]

replace_values_cpc = {
}

replace_values_enade = {
    "categoria_administrativa": {
        "Pessoa Jurídica de Direito Público - Federal": "Pública Federal",
        "Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Civil": "Privada com fins lucrativos",
        "Pessoa Jurídica de Direito Público - Estadual": "Pública Estadual",
        "Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Associação de Utilidade Pública": "Privada sem fins lucrativos",
        "Federal": "Pública Federal",
        "Estadual": "Pública Estadual",
        "Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Fundação": "Privada sem fins lucrativos",
        "Pessoa Jurídica de Direito Público - Municipal": "Pública Municipal",
        "Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Mercantil ou Comercial": "Privada com fins lucrativos",
        "Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Sociedade": "Privada sem fins lucrativos",
        "Fundação Pública de Direito Privado Municipal": "Privada sem fins lucrativos",
        "Administração pública em geral": "Pública Municipal"
    }
}

sql_dtypes = {
    "codigo_do_curso": sqlalchemy.types.Text,
    "proporcao_de_concluintes_participantes_com_nota_no_enem": sqlalchemy.types.Text,
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
                       encoding="latin1", decimal=",", thousands=".",
                       na_values=na_values_cpc, **kwargs)

def transform_indicadores(df, year):
    df.dropna(axis = 0, how = "all", inplace=True)
    df.dropna(axis = 1, how = "all", inplace=True)
    #df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.columns = utils.clean_col_names(df.columns)
    cols_to_rename = utils.filter_dict_by_keys(rename_columns_cpc, df.columns)
    if cols_to_rename:
        print(f"Renaming columns: {cols_to_rename}")
        df.rename(columns=cols_to_rename, inplace=True)
    if 'ano' not in df.columns:
        df['ano'] = year
    df.replace(replace_values_cpc, inplace=True)
    return df

def load_indicadores(df, csv_file, db_con, sql_table, sql_schema):
    print(f"Loading {csv_file} to {sql_schema}.{sql_table}")
    cur_cols = utils.list_db_column_names(db_con, sql_table, sql_schema)
    new_cols = df.columns.difference(cur_cols)
    if not cur_cols.empty and not new_cols.empty:
        print(f"New columns found: {new_cols}")
        utils.add_db_table_columns(new_cols, "text", db_con, sql_table, sql_schema)
        missing_cols = cur_cols.difference(df.columns)
        print(f"Missing columns: {missing_cols}")
    df.to_sql(sql_table, db_con, sql_schema, index=False, if_exists='append',
              dtype=sql_dtypes)

def etl_indicadores(years, db_url, sql_table, sql_schema, data_dir="data"):
    db_con = setup_db(db_url, sql_table, sql_schema)
    for year in years:
        conf = cpc_conf[year]
        csv_file = os.path.join(data_dir, f"inep_cpc_{year}.csv")
        download_indicadores(conf["url"], csv_file)
        extract_kwargs = conf["extract_kwargs"] if "extract_kwargs" in conf else {}
        df = extract_indicadores(csv_file, **extract_kwargs)
        df = transform_indicadores(df, year)
        print(df.columns)
        load_indicadores(df, csv_file, db_con, sql_table, sql_schema)

def main(args):
    years = cpc_conf.keys() if len(args) == 0 else args
    data_dir = "data"
    db_url = utils.build_db_url(
        "postgresql", os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PWD"),
        os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_DB")
    )
    sql_table = "indicadores"
    sql_schema = "inep"
    etl_indicadores(years, db_url, sql_table, sql_schema, data_dir)

if __name__ == '__main__':
    main(sys.argv[1:])
