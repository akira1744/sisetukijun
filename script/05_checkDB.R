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

################################################################################

# すべてのrdsを格納するdb
all_db_path <- str_glue('sisetukijun_all.sqlite') %>% print()

# sqliteにconnect
all_db_con <- DBI::dbConnect(RSQLite::SQLite(), all_db_path) 

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) %>% print()

################################################################################

db_path <- str_glue('sisetukijun.sqlite') %>% print()

# sqliteにconnect
con <- DBI::dbConnect(RSQLite::SQLite(), db_path) 

# table一覧を確認
DBI::dbListTables(con)

################################################################################

tbl(con,'mst_sisetu') %>% 
  filter(都道府県名=='埼玉県') %>% 
  distinct(update_date)

# 施設一覧
mst_sisetu <- tbl(con,'mst_sisetu') %>% 
  collect() %>% 
  glimpse()

# 施設基準一覧
sisetukijun <- tbl(con,'sisetukijun') %>% 
  collect() %>% 
  glimpse()

# 特定の病院の施設マスタと施設基準を確認
mst_sisetu %>% 
  filter(str_detect(都道府県名,'埼玉県')) %>% 
  filter(str_detect(医療機関名称,'西埼玉')) %>% 
  left_join(sisetukijun,by=c('update_date','医療機関コード')) %>% 
  glimpse()


