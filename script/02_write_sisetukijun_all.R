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

output_dir <- here('output')

# すべてのrdsを格納するdb
all_db_path <- here('sisetukijun_all.sqlite') 

# sqliteにconnect
all_db_con <- DBI::dbConnect(RSQLite::SQLite(), all_db_path) 

# 書き込み権限を変更
system(paste("sudo chmod -R 666", all_db_path))

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) 

################################################################################

# 読み込みファイル一覧
target_files <- list.files(output_dir, full.names = TRUE,pattern = 'df_all.rds',recursive = TRUE) %>% 
  print()

# sisetukijun_all_get_dateが存在したら
if ('sisetukijun_all_get_date' %in% all_db_tables){
  # get_の一覧を取得
  all_get_date_df <- tbl(all_db_con, 'sisetukijun_all_get_date') %>% 
    distinct(get_date) %>% 
    collect() %>% 
    print()
  
  all_get_dates <- str_replace_all(all_get_date_df$get_date,'-','')
  
  # file名に各 all_get_dates を含むファイルを除外
  for(date in all_get_dates) {
    target_files <- str_subset(target_files, date, negate = TRUE)
  }

}

print(target_files)

################################################################################

# filesを1つずつ読み込んでdbに書き込む
for(file in target_files){
  # rdsを読み込み
  df_all <- readRDS(file)
  
  # 日付型を文字列に変換
  df_all <- df_all %>% 
    mutate(across(where(~ inherits(.x,'Date')), ~ as.character(.x))) 
  
  # tmpテーブルに書き込み
  DBI::dbWriteTable(all_db_con, 'sisetukijun_all_get_date', df_all, append = TRUE) %>% print()
}

################################################################################

# update_dateが同じでget_dateが異なるデータが複数あるので、
# update_dateが同じデータの中で、get_dateが最も新しいものを残すことにする
# 全データをメモリに乗せることができないので、update_date毎に処理する

################################################################################

# update_dateの一覧を取得
all_update_date_df <- tbl(all_db_con, 'sisetukijun_all_get_date') %>% 
  distinct(update_date) %>% 
  collect() %>% 
  print()

################################################################################

# sisetukijun_all_update_dateが存在したら削除(このテーブルは毎回すべてやり直す)
DBI::dbExecute(all_db_con,'DROP TABLE IF EXISTS sisetukijun_all_update_date;')

################################################################################

# update_dateをループ処理
for (select_date in all_update_date_df$update_date){
  
  print(select_date)
  
  # update_dateの中でget_dateが最新のdataを取得
  tmp <- tbl(all_db_con, 'sisetukijun_all_get_date') %>% 
    filter(update_date == select_date) %>% 
    group_by(update_date,厚生局) %>%
    filter(get_date == max(get_date,na.rm=T)) %>% 
    ungroup() %>%
    collect()
  
  # sisetukijun_allテーブルに書き込む
  DBI::dbWriteTable(all_db_con, 'sisetukijun_all_update_date', tmp, append = TRUE) %>% print()
}

################################################################################

# 切断
DBI::dbDisconnect(all_db_con)

################################################################################
