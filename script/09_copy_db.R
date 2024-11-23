library(tidyverse)

# sisetukijun.duckdbをfacility-standardsにcopy
src_path <- '/home/rstudio/srv/project/sisetukijun/sisetukijun.duckdb'
dst_path <- '/home/rstudio/srv/shinyapps/facility-standards/sisetukijun.duckdb'

# ファイルコピーを試みる
if (file.exists(src_path)) {
  success <- file.copy(src_path, dst_path, overwrite = TRUE)
  
  if (success) {
    message(str_glue("{src_path}を{dst_path}にコピーしました"))
  } else {
    message(str_glue("{src_path}のコピーに失敗しました。"))
  }
} else {
  message("コピー元のファイルが見つかりません: ", src_path)
}
