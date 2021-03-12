library(dplyr)
library(here)
library(readr)

download.file("https://github.com/wcota/covid19br/raw/master/cities_info.csv",
              here("data/cities_info.csv"))

read_csv(here("data", "cities_info.csv")) %>%
  transmute(
    cod_municipio = ibge,
    nome_municipio = str_extract(city, "[^/]+"),
    uf = state,
    regiao = region,
    populacao = pop2020
  ) %>%
  arrange(cod_municipio) %>%
  write_csv(here("data", "municipios_info.csv"))
