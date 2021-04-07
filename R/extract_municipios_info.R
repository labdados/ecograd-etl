library(dplyr)
library(here)
library(readr)
library(stringr)

download.file("https://github.com/wcota/covid19br/raw/master/cities_info.csv",
              here("data/cities_info.csv"))

mun_info <- read_csv(here("data", "cities_info.csv")) %>%
  transmute(
    cod_municipio = ibge,
    nome_municipio = str_extract(city, "[^/]+"),
    uf = state,
    regiao = region,
    populacao = pop2020
  ) %>%
  arrange(cod_municipio) 
  
write_csv(mun_info, here("data", "municipios_info.csv"))

states_info <- mun_info %>%
  group_by(uf) %>%
  summarise(
    cod_estado = str_sub(cod_municipio, end = 2),
    regiao = first(regiao),
    populacao = sum(populacao, na.rm = TRUE)
  )

write_csv(states_info, here("data", "estados_info.csv"))