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

################################################################################

# sisetukijun_all_update_dateが存在したら削除(このテーブルは毎回すべてやり直す)
DBI::dbExecute(all_db_con,'DROP TABLE IF EXISTS sisetukijun_all_update_date;')


################################################################################

# 厚生局ごとのupdate_dateと最新のget_dateを取得
max_get_date_df <- tbl(all_db_con, 'sisetukijun_all_get_date') %>% 
  group_by(厚生局,update_date) %>% 
  summarise(max_get_date = max(get_date,na.rm=T)) %>% 
  ungroup() %>% 
  collect() %>% 
  print()

# 1行ごとループして、dbに書き込み
for (i in 1:nrow(max_get_date_df)){
  
  target_kouseikyoku <- max_get_date_df$厚生局[i] 
  target_update_date <- max_get_date_df$update_date[i]
  target_get_date <- max_get_date_df$max_get_date[i]
  
  tmp <- tbl(all_db_con, 'sisetukijun_all_get_date') %>% 
    filter(
      厚生局 == target_kouseikyoku
      ,update_date == target_update_date
      ,get_date == target_get_date
    ) %>% 
    collect()
  
  # sisetukijun_allテーブルに書き込む
  DBI::dbWriteTable(all_db_con, 'sisetukijun_all_update_date', tmp, append = TRUE) 
  rm(tmp)
}

################################################################################

# 切断
DBI::dbDisconnect(all_db_con)

################################################################################

# 完了
message('02_write_sisetukijun_all.Rが完了しました。')
