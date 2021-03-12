library(dplyr)
library(here)
library(readr)

read_csv("https://github.com/wcota/covid19br/raw/master/cities_info.csv") %>%
  transmute(
    cod_municipio = ibge,
    municipio = toupper(str_extract(city, "[^/]+")),
    uf = state,
    regiao = region,
    populacao = pop2020
  ) %>%
  arrange(cod_municipio) %>%
  write_csv(here("data", "municipios_info.csv"))
