from ecograd_etl import utils 
import sqlalchemy

conf = {
    "data_dir": "data",
    "sql_schema": "inep",
    "datasets": {
        "cpc": {
            "sql_table": "cpc",
            "items": {
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
                # "2014": {
                #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2014/cpc2014_atualizado_em_04122017.csv"
                # },
                # "2013": {
                #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2013/cpc2013_atualizado_em_27112017.csv"        
                # },
                # "2012": {
                #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2012/cpc_2012_site_2014_03_14.csv"
                # },
                # "2011": {
                #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2011/tabela_enade_cpc_2011_retificado_08_02_13.csv"
                # },
                # "2010": {
                #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2010/tabela_enade_cpc_2010.csv"
                # }
            }
        },
        "enade": {
            "sql_table": "enade",
            "items": {
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
                }#,
                # "2014": {
                #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2014/conceito_enade_2014_atualizado_em_04122017.csv"
                # },
                # "2013": {
                #     "url": "https://download.inep.gov.br/educacao_superior/enade/planilhas/2013/conceito_enade_2013.csv"
                #},
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
        }
    },
    "rename_columns": {
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
        "nota_padronizada_do_idd": "nota_padronizada_idd",
        "nota_padronizada_infraestrutura": "nota_padronizada_infraestrutura_e_instalacoes_fisicas",
        "nota_padronizada_oport_ampliacao": "nota_padronizada_oportunidade_de_ampliacao_da_formacao",
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
    },
    "replace_values": {
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
    },
    "na_values": [
        "", "-", ".",
        "Resultado desconsiderado devido à Política de Transferência Assistida (Portaria MEC nº 24/2016)"
    ],
    "dtype": {
        "codigo_do_curso": sqlalchemy.types.Text,
        "proporcao_de_concluintes_participantes_com_nota_no_enem": sqlalchemy.types.Text
    },
    "converters": {
        "cpc_continuo": utils.parse_float
    }
}