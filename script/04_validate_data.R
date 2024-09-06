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

# すべてのrdsを格納するdb
db_path <- str_glue('sisetukijun.sqlite') %>% print()

# sqliteにconnect
db_con <- DBI::dbConnect(RSQLite::SQLite(), db_path) 

# table一覧を確認
db_tables <- DBI::dbListTables(db_con) %>% print()

################################################################################

# 厚生局ごとにいつのデータが入っているかのデータを作る
kouseikyoku_update_date <- tbl(db_con,'mst_sisetu') %>% 
  distinct(厚生局,update_date) %>% 
  collect() 

# 厚生局を縦,年月を横に展開
agg_kouseikyoku_update_date <- kouseikyoku_update_date %>% 
  mutate(年月 = str_sub(str_replace_all(update_date,'-',''),1,6)) %>%
  mutate(cnt=1) %>% 
  arrange(年月) %>% 
  pivot_wider(
    id_cols='厚生局'
    ,names_from='年月'
    ,values_from='cnt'
    ,values_fill=0) %>% 
  arrange(厚生局) 

# 出力
agg_kouseikyoku_update_date %>% 
  write_xlsx('DB格納データ一覧.xlsx')

system("sudo chmod 666 DB格納データ一覧.xlsx")

################################################################################

# 西暦算定開始年月日がデータ取得日以降
check1 <- tbl(all_db_con,'sisetukijun_all_get_date') %>% 
  filter(西暦算定開始年月日 > get_date) %>% 
  collect()

glimpse(check1)

print(str_glue('不正データチェック: 算定開始年月日 > get_date: {nrow(check1)}件'))

################################################################################

DBI::dbDisconnect(all_db_con)
