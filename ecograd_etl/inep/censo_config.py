from ecograd_etl import utils 
import sqlalchemy

conf = {
    "data_dir": "data",
    "sql_schema": "dev_inep_censo",
    "datamart_schema": "dev_inep_censo",
    "datasets": {
        "microdados_censo": {
            "tables": {
                "curso": {
                    "table_name": "curso",
                    "file_prefix": "SUP_CURSO",
                    "file_extension": "csv"
                }
            },
            "items": {
                "2019": {
                    "url": "https://download.inep.gov.br/microdados/microdados_educacao_superior_2019.zip",
                    "filename": "microdados_censo_superior_2019.zip"
                },
                "2018": {
                    "url": "https://download.inep.gov.br/microdados/microdados_educacao_superior_2018.zip",
                    "filename": "microdados_censo_superior_2018.zip"
                },
                "2017": {
                    "url": "https://download.inep.gov.br/microdados/microdados_educacao_superior_2017.zip",
                    "filename": "microdados_censo_superior_2017.zip"
                },
                "2016": {
                    "url": "https://download.inep.gov.br/microdados/microdados_censo_superior_2016.zip",
                    "filename": "microdados_censo_superior_2016.zip"
                },
                "2015": {
                    "url": "https://download.inep.gov.br/microdados/microdados_censo_superior_2015.zip",
                    "filename": "microdados_censo_superior_2015.zip"
                },
                "2014": {
                    "url": "https://download.inep.gov.br/microdados/microdados_censo_superior_2014.zip",
                    "filename": "microdados_censo_superior_2014.zip"
                },
                #"2013": {
                #    "url": "https://download.inep.gov.br/microdados/microdados_censo_superior_2013.zip",
                #    "filename": "microdados_censo_superior_2013.zip"
                #},
                #"2012": {
                #    "url": "https://download.inep.gov.br/microdados/microdados_censo_superior_2012.zip",
                #    "filename": "microdados_censo_superior_2012.zip"
                #}
            }
        }
    },
    "rename_columns": {
        "alfa": "alfa_proporcao_de_graduacao",
        "alfa_proporcao_de_graduandos": "alfa_proporcao_de_graduacao",
        "ano_enade": "ano",
        "area": "area_de_avaliacao",
        "area_de_enquadramento": "area_de_avaliacao",
        "area_enquadramento": "area_de_avaliacao",
        "beta": "beta_proporcao_de_mestrado_equivalente",
        "beta_proporcao_de_mestrandos_equivalente": "beta_proporcao_de_mestrado_equivalente",
        "categ_administrativa": "categoria_administrativa",
        "co_ies": "codigo_da_ies",
        "cod_area": "codigo_da_area",
        "cod_cursos_da_unidade": "codigo_do_curso",
        "cod_ies": "codigo_da_ies",
        "cod_municipio": "codigo_do_municipio",
        "codigo_area": "codigo_da_area",
        "codigo_ies": "codigo_da_ies",
        "codigo_municipio": "codigo_do_municipio",
        "codigo_uf": "sigla_da_uf",
        "conceito_enade": "conceito_enade_faixa",
        "conceito_medio_da_graduacao": "conceito_medio_de_graduacao",
        "conceito_medio_do_mestrado": "conceito_medio_de_mestrado",
        "concluintes_inscritos": "n_de_concluintes_inscritos",
        "concluintes_participantes": "n_de_concluintes_participantes",
        "conluintes_inscritos": "n_de_concluintes_inscritos",
        "d": "conceito_medio_do_doutorado",
        "de_doutores": "nota_bruta_doutores",
        "de_mestres": "nota_bruta_mestres",
        "edicao": "ano",
        "fx_igc": "igc_faixa",
        "g_ies": "conceito_medio_de_graduacao",
        "gama": "gama_proporcao_de_doutorandos_equivalente",
        "ies": "nome_da_ies",
        "igc": "igc_continuo",
        "inscritos": "n_de_concluintes_inscritos",
        "m": "conceito_medio_de_mestrado",
        "n_de_concluintes_participantes_com_nota_no_enem": "concluintes_participantes_com_nota_no_enem",
        "n_cursos_cpc": "n_de_cursos_com_cpc_no_trienio",
        "no_categad": "categoria_administrativa",
        "no_ies": "nome_da_ies",
        "no_orgacad": "organizacao_academica",
        "nome_do_municipio": "municipio_do_curso",
        "nota_bruta_do_idd": "nota_bruta_idd",
        "nota_bruta_infraestrutura": "nota_bruta_infraestrutura_e_instalacoes_fisicas",
        "nota_bruta_oport_ampliacao": "nota_bruta_oportunidade_de_ampliacao_da_formacao",
        "nota_bruta_oportunidades_de_ampliacao_da_formacao": "nota_bruta_oportunidade_de_ampliacao_da_formacao",
        "nota_bruta_org_didatico_pedagogica": "nota_padronizada_organizacao_didatico_pedagogica",
        "nota_continua_do_enade": "conceito_enade_continuo",
        "nota_padronizada_de_doutores": "nota_padronizada_doutores",
        "nota_padronizada_de_mestres": "nota_padronizada_mestres",
        "nota_padronizada_de_regime_de_trabalho": "nota_padronizada_regime_de_trabalho",
        "nota_padronizada_idd": "idd_continuo",
        "nota_padronizada_do_idd": "nota_padronizada_idd",
        "nota_padronizada_infraestrutura": "nota_padronizada_infraestrutura_e_instalacoes_fisicas",
        "nota_padronizada_oport_ampliacao": "nota_padronizada_oportunidade_de_ampliacao_da_formacao",
        "nota_padronizada_oportunidades_de_ampliacao_da_formacao": "nota_padronizada_oportunidade_de_ampliacao_da_formacao",
        "nota_padronizada_org_didatico_pedagogica": "nota_padronizada_organizacao_didatico_pedagogica",
        "nr_cursos_da_unidade": "numero_de_cursos_da_unidade",
        "nr_de_docentes": "n_de_docentes",
        "nr_de_cursos_com_cpc_no_trienio": "n_de_cursos_com_cpc_no_trienio",
        "nu_ano": "ano",
        "nu_ano_censo": "ano",
        "obs": "observacao",
        "org_academica": "organizacao_academica",
        "participantes": "n_de_concluintes_participantes",
        "percentual_de_concluintes_participantes_com_nota_no_enem": "proporcao_de_concluintes_participantes_com_nota_no_enem",
        "sigla_ies": "sigla_da_ies",
        "sigla_uf": "sigla_da_uf",
        "sg_ies": "sigla_da_ies",
        "sg_uf": "sigla_da_uf",
        "uf_da_ies": "sigla_da_uf",
        "uf_do_curso": "sigla_da_uf"
    },
    "replace_values": {
        "categoria_administrativa": {
            "Administração pública em geral": "Pública Municipal",
            "Estadual": "Pública Estadual",
            "Federal": "Pública Federal",
            "Fundação Pública de Direito Privado Municipal": "Pública Municipal",
            "Pessoa Jurídica de Direito Privado - Com fins lucrativos - Associação de Utilidade Pública": "Privada com fins lucrativos",
            "Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Civil": "Privada com fins lucrativos",
            "Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Mercantil ou Comercial": "Privada com fins lucrativos",
            "Pessoa Jurídica de Direito Público - Estadual": "Pública Estadual",
            "Pessoa Jurídica de Direito Público - Federal": "Pública Federal",
            "Pessoa Jurídica de Direito Público - Municipal": "Pública Municipal",
            "Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Associação de Utilidade Pública": "Privada sem fins lucrativos",
            "Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Fundação": "Privada sem fins lucrativos",
            "Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Sociedade": "Privada sem fins lucrativos"
        }
    },
    "na_values": [
        "", "-", ".",
        "Resultado desconsiderado devido à Política de Transferência Assistida (Portaria MEC nº 24/2016)",
        "Sub judice"
    ],
    "dtype": {
        "codigo_do_curso": sqlalchemy.types.Text,
        "proporcao_de_concluintes_participantes_com_nota_no_enem": sqlalchemy.types.Text
    },
    "converters": {
        "cpc_continuo": utils.parse_float,
        "codigo_do_curso": round
    }
}
