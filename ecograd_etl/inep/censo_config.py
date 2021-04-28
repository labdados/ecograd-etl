from ecograd_etl import utils 
import sqlalchemy

conf = {
    "data_dir": "data",
    "datasets": {
        "microdados_censo": {
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
                "2013": {
                    "url": "https://download.inep.gov.br/microdados/microdados_censo_superior_2013.zip",
                    "filename": "microdados_censo_superior_2013.zip"
                },
                "2012": {
                    "url": "https://download.inep.gov.br/microdados/microdados_censo_superior_2012.zip",
                    "filename": "microdados_censo_superior_2012.zip"
                }
            }
        }
    }
}
