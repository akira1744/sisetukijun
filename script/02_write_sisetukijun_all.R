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

################################################################################

output_dir <- here('output') %>% print()

# すべてのrdsを格納するdb
all_db_path <- 'sisetukijun_all.duckdb'

# すべてのget_dateのdataを格納するDBを用意
all_db_con <- DBI::dbConnect(duckdb::duckdb(),all_db_path,read_only=FALSE)

# 書き込み権限を変更
system(paste("sudo chmod -R 666", all_db_path))

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) %>% print()

################################################################################

# 読み込みファイル一覧
target_files <- list.files(
  output_dir
  ,full.names = TRUE
  ,pattern = 'df_all.rds'
  ,recursive = TRUE
  ) %>% 
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
  DBI::dbWriteTable(all_db_con, 'sisetukijun_all_get_date', df_all, append = TRUE)
}

################################################################################

# indexがなくても十分早いし、indexの作成は時間がかかるのでアプリで使う部分だけにする。

################################################################################


# sisetukijun_all_get_dateのupdate_date列にindexを作成（存在しない場合のみ）

# idx_name <- 'idx_sisetukijun_all_get_date_update_date'
# 
# query <- str_glue(
#   'CREATE INDEX {idx_name} ON sisetukijun_all_get_date(update_date);'
#   )
# 
# start_time <- proc.time()
# 
# # すでにindex tableが存在するとErrorが発生するのでerror処理
# result <- tryCatch({
#   DBI::dbExecute(all_db_con, query)
#   message(str_glue('インデックス{idx_name}を新規作成しました。'))
#   TRUE
# }, error = function(e) {
#   message(str_glue('インデックス{idx_name}の新規作成に失敗しました。エラー: {e$message}'))
#   FALSE
# })
# 
# end_time <- proc.time()
# elapsed_time <- end_time - start_time
# message(glue('処理にかかった時間: {elapsed_time[3]} 秒'))

################################################################################

# update_dateが同じでget_dateが異なるデータが複数あるので、
# update_dateが同じデータの中で、get_dateが最も新しいものを残すことにする
# 全データをメモリに乗せることができないので、update_date毎に処理する

################################################################################

# update_dateの一覧を取得
all_update_date_df <- tbl(all_db_con, 'sisetukijun_all_get_date') %>% 
  distinct(update_date) %>% 
  collect() %>% 
  arrange(update_date) %>% 
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
  DBI::dbWriteTable(all_db_con, 'sisetukijun_all_update_date', tmp, append = TRUE) 
}

################################################################################

# 切断
DBI::dbDisconnect(all_db_con)

################################################################################

# 完了
message('02_write_sisetukijun_all.Rが完了しました。')
