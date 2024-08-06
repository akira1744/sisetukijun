rm(list=ls())

pacman::p_load(
  rvest
  ,httr
  ,readxl
  ,writexl
  ,openxlsx
  ,zipangu
  ,lubridate
  ,tidyverse
  ,tidylog
)


################################################################################

df_all <- readRDS('output/20240806/df_all.rds')

# 病床数を"／"で分割してlistにする
df_all_bed <- df_all %>% 
  distinct(update_date,都道府県コード,都道府県名,医療機関番号,病床数) %>% 
  mutate(tmp = str_split(病床数, '／')) %>%
  unnest(cols = c(tmp)) %>%
  mutate(病床区分 = str_extract(tmp,'[^0-9]+')) %>%
  mutate(病床区分 = str_replace_all(病床区分,'　','')) %>%
  mutate(bed = str_extract(tmp,'[0-9]+$')) %>% 
  mutate(病床区分 = case_when(
    病床数=='' ~ 'なし'
    ,is.na(病床数) ~ 'なし'
    ,病床区分 == '' ~ 'なし'
    ,is.na(病床区分) ~ 'なし'
    ,T ~ 病床区分
  )) %>% 
  mutate(bed = case_when(
    病床数=='' ~ '0'
    ,is.na(病床数) ~ '0'
    ,is.na(bed) ~ '0'
    ,病床区分=='なし' ~ '0'
    ,T ~ bed
  )) %>%
  select(-tmp) %>% 
  # select(-病床数) %>%
  print()

# # 名称の微調整
df_all_bed <- df_all_bed %>%
  mutate(病床区分 = case_when(
    病床区分 == '一般一般' ~ '一般'
    ,病床区分 == '介護介護' ~ '介護'
    ,病床区分 == '療養療養' ~ '療養'
    ,T ~ 病床区分
  ))

df_all_bed

