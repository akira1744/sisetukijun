rm(list=ls())

pacman::p_load(
  here
  ,DBI
  ,writexl
  ,lubridate
  ,duckdb
  ,duckplyr
  ,tidyverse
  ,tidylog
)

output_dir <- here('output') %>% print()

################################################################################

# すべてのrdsを格納するdb
all_db_path <- 'sisetukijun_all.duckdb'

# connect
all_db_con <- dbConnect(duckdb(),all_db_path,read_only=FALSE)

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) %>% print()

################################################################################

# normalize後のdataを格納するdb
db_path <- 'sisetukijun.duckdb'

con <- dbConnect(duckdb(),db_path,read_only=FALSE)

# table一覧を確認
db_tables <- DBI::dbListTables(con) %>% print()

################################################################################

tbl(con,'mst_update_date')

