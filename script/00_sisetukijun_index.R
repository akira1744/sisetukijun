rm(list=ls())

pacman::p_load(
  here
  ,tidyverse
  ,tidylog
)

# 現在の日時を取得してlogファイル名を作成
log_file <- str_glue(
  'log/{format(Sys.time(), "%Y%m%d-%H%M%S.log")}')%>%
  print()

# 標準出力をログファイルにリダイレクト
sink(log_file, append=TRUE)

# echo=TRUEを使用してsource()の中で実行されるコマンドを印字
source('script/01_get_sisetukijun.R', echo=TRUE)
source('script/01.1_make_df_all_rds.R', echo=TRUE)
source('script/02_write_sisetukijun_all.R', echo=TRUE)
source('script/03_normalizeDB.R', echo=TRUE)
source('script/04_validate_data.R', echo=TRUE)

# 標準出力を元に戻す
sink()

message('すべての処理が完了しました')
