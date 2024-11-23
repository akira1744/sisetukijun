rm(list=ls())

pacman::p_load(
  rvest
  ,here
  ,httr
  ,openxlsx
  ,zipangu
  ,arrow
  ,tidyverse
  ,tidylog
)


################################################################################

files <- list.files(
  'output'
  ,full.names = TRUE
  ,recursive = TRUE
  ,pattern = 'dates.csv$'
) 

files

# 読み込んで列を絞り込む関数
read_dates_csv <- function(file){
  df <- read_csv(file) %>%
    select(date,kousei,update_date)
  return(df)
}

# 読み込んで縦結合
df <- tibble(file = files) %>% 
  mutate(data = map(file,read_dates_csv)) %>% 
  select(data) %>% 
  unnest(cols = data) %>% 
  print()

# kouseiを横に展開
df_wide <- df %>% 
  pivot_wider(
    id_cols = date
    ,names_from = kousei
    ,values_from = update_date
  ) %>% 
  print()

df_wide %>% names() %>% dput()

df_wide <- df_wide %>% 
  select("date", "北海道", "東北", "関東信越", "東海北陸", "四国", "近畿", "中国", "九州") %>% 
  print()

# 書き出し
write_csv(df_wide, 'output/更新日.csv')


