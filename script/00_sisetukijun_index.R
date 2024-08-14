rm(list=ls())

pacman::p_load(
  here
  ,tidyverse
  # ,tidylog
)

source('script/01_get_sisetukijun.R')

source('script/02_write_sisetukijun_all.R')

source('script/03_normalizeDB.R')

source('script/04_validate_data.R')

print('Done')

