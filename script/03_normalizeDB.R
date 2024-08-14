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

db_path <- str_glue('sisetukijun.sqlite') %>% print()

# sqliteにconnect
con <- DBI::dbConnect(RSQLite::SQLite(), db_path) 

# 書き込み権限を変更
system(paste("sudo chmod -R 666", db_path))

################################################################################

# mst_prefテーブルが存在したらdropする
DBI::dbExecute(con,'DROP TABLE IF EXISTS mst_pref;')

# mst_sisetuテーブルが存在したらdropする
DBI::dbExecute(con,'DROP TABLE IF EXISTS mst_sisetu;')

# sisetukijunテーブルが存在したらdropする
DBI::dbExecute(con,'DROP TABLE IF EXISTS sisetukijun;')

################################################################################

# サンプル確認
sample <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% head() %>% collect() %>% glimpse()
sample %>% names() %>% dput()

################################################################################

# 都道府県一覧を作成
mst_pref <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
  distinct(都道府県コード,都道府県名) %>% 
  collect() %>% 
  arrange(都道府県コード) %>% 
  print()

# DBにmst_prefを書き込み
DBI::dbWriteTable(con, 'mst_pref', mst_pref, overwrite=T) %>% print()

# 確認
tbl(con, 'mst_pref') %>% print()

################################################################################

# update_dateの一覧を取得
update_dates <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
  distinct(update_date) %>% 
  collect() %>% 
  print()

################################################################################

# update_dateをループ処理
for (select_date in update_dates$update_date){
  
  print(select_date)
  
  # 病院一覧を取得
  mst_sisetu  <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
    filter(update_date==select_date) %>%
    select(
      update_date
      ,医療機関コード
      # ,都道府県コード
      ,都道府県名
      # ,区分
      # ,医療機関番号
      ,併設医療機関番号
      # ,医療機関記号番号
      ,医療機関名称
      ,郵便番号=`医療機関所在地（郵便番号）`
      ,住所 = `医療機関所在地（住所）`
      ,電話番号
      ,FAX番号
      ,病床数
    ) %>% 
    distinct() %>% 
    collect() %>% 
    glimpse()
  
  # sisetukijun_allテーブルに書き込む
  DBI::dbWriteTable(con, 'mst_sisetu', mst_sisetu, append = TRUE) %>% print()
}

################################################################################

# update_dateをループ処理
for (select_date in update_dates$update_date){
  
  print(select_date)
  
  # 施設基準一覧を取得
  sisetukijun  <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
    filter(update_date==select_date) %>%
    select(
      update_date
      ,医療機関コード
      ,受理届出名称
      # ,受理記号
      # ,受理番号
      ,西暦算定開始年月日
    ) %>% 
    collect() %>% 
    print()
  
  # sisetukijun_allテーブルに書き込む
  DBI::dbWriteTable(con, 'sisetukijun', sisetukijun, append = TRUE) %>% print()
}

################################################################################

# 切断
DBI::dbDisconnect(all_db_con)
DBI::dbDisconnect(con)

################################################################################
