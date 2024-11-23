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

# normalize後のdataを格納するdb
db_path <- here('sisetukijun.duckdb') %>% print()

# connect
con <- dbConnect(duckdb(),db_path,read_only=FALSE)

# table一覧を確認
db_tables <- DBI::dbListTables(con) %>% print()

# 2024年8月の施設一覧を取得
sisetu <- tbl(con,'sisetu_main') %>% 
  left_join(tbl(con,'sisetu_sub'),by=c('update_date','医療機関コード')) %>% 
  select(update_date,医療機関コード,医療機関名称,病床数) %>% 
  collect() %>% 
  glimpse()

sisetu <- sisetu %>% 
  mutate(bed=str_split(病床数,'／')) %>% 
  unnest(bed) %>% 
  mutate(bed_number = as.numeric(str_extract(bed,'[0-9]+'))) %>%
  print()

sisetu_bed <- sisetu %>% 
  group_by(update_date,医療機関コード,医療機関名称) %>% 
  summarise(bed = sum(bed_number,na.rm=TRUE)) %>%
  ungroup() %>% 
  print()

sisetu_bed <- sisetu_bed %>% 
  select(update_date,医療機関コード,bed) %>% 
  print()

# 書き込み
dbWriteTable(con,'sisetu_bed',sisetu_bed,overwrite=TRUE)

dbDisconnect(con)

################################################################################


