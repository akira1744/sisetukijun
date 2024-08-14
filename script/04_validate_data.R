rm(list=ls())

pacman::p_load(
  here
  ,DBI
  ,RSQLite
  ,writexl
  ,lubridate
  ,tidyverse
  # ,tidylog
)

output_dir <- here('output') %>% print()

################################################################################

# すべてのrdsを格納するdb
all_db_path <- str_glue('sisetukijun_all.sqlite') %>% print()

# sqliteにconnect
all_db_con <- DBI::dbConnect(RSQLite::SQLite(), all_db_path) 

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) %>% print()

################################################################################

# 西暦算定開始年月日がデータ取得日以降
check1 <- tbl(all_db_con,'sisetukijun_all_get_date') %>% 
  filter(西暦算定開始年月日 > get_date) %>% 
  collect()

print(str_glue('不正データチェック: 算定開始年月日 > get_date: {nrow(check1)}件'))

glimpse(check1)

################################################################################

DBI::dbDisconnect(all_db_con)
