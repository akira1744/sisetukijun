rm(list=ls())

pacman::p_load(
  here
  ,DBI
  ,duckdb
  ,duckplyr
  ,writexl
  ,arrow
  ,lubridate
  ,tidyverse
  ,tidylog
)

output_dir <- here('output') %>% print()

################################################################################

# すべてのparquetを格納するdb
all_db_path <- here('sisetukijun_all.duckdb') %>% print()

# connect
all_db_con <- dbConnect(duckdb(),all_db_path,read_only=FALSE)

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) %>% print()

################################################################################

# normalize後のdataを格納するdb
db_path <- here('sisetukijun.duckdb') %>% print()

# connect
con <- dbConnect(duckdb(),db_path,read_only=FALSE)

# table一覧を確認
db_tables <- DBI::dbListTables(con) %>% print()

################################################################################

# 西暦算定開始年月日がデータ取得日以降
check1 <- tbl(all_db_con,'sisetukijun_all_get_date') %>% 
  filter(西暦算定開始年月日 > get_date) %>% 
  collect()

glimpse(check1)

print(str_glue('不正データチェック: 算定開始年月日 > get_date: {nrow(check1)}件'))

################################################################################

DBI::dbDisconnect(all_db_con)
DBI::dbDisconnect(con)
