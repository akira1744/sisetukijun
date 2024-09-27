rm(list=ls())

pacman::p_load(
  here
  ,arrow
  ,tidyverse
  ,tidylog
)

# 現在の日時を取得してlogファイル名を作成
log_file <- str_glue(
  'log/{format(Sys.time(), "%Y%m%d-%H%M%S.log")}')%>%
  print()

log_file_connection <- file(log_file, open='a')

# 標準出力をログファイルにリダイレクト
sink(log_file_connection)

print(str_glue('log start: {Sys.time()}'))

sink(log_file_connection, type='message')

# echo=TRUEを使用してsource()の中で実行されるコマンドを印字
source('script/01_get_sisetukijun.R', echo=TRUE)
source('script/01.1_make_df_all_parquet.R', echo=TRUE)
source('script/02_write_sisetukijun_all.R', echo=TRUE)
source('script/03_normalizeDB.R', echo=TRUE)
source('script/04_validate_data.R', echo=TRUE)

message('すべての処理が完了しました')

print(str_glue('log end: {Sys.time()}'))

# 標準出力を元に戻す
sink()

sink(type='message')


rm(list=ls())
