rm(list=ls())

pacman::p_load(
  rvest
  ,here
  ,httr
  ,openxlsx
  ,zipangu
  ,arrow
  ,tidyverse
  ,tidylog
)

################################################################################

# output_dirのpath
today_ymd <- str_replace_all(Sys.Date(),'-','') %>% print()
output_dir <- here(str_glue('output/{today_ymd}')) %>% print()

# TODO 手動でparquet出力をやりなおしする場合はコメントアウト
# output_dir <- 'output/20240829'

# 書き込み権限を変更
system(paste("sudo chmod -R 777", output_dir))

################################################################################

original_dir <- here(str_glue('{output_dir}/original')) %>% print()

# ファイル一覧
files <- list.files(original_dir, full.names = TRUE, recursive = TRUE,pattern = '.xlsx$') %>% 
  print()

# 関数テスト用
# file <- files[1]
# sheet <- 'Sheet1'

# 読み込み用関数
read_sisetukijun_xlsx<- function(file, sheet){
  readxl::read_excel(file, skip=3, sheet = sheet,col_types='text') %>% 
    tibble() 
}

dplyr::tibble(file = files) %>% 
  mutate(sheet = map(file, openxlsx::getSheetNames)) %>% 
  unnest(cols = c(sheet)) %>% 
  mutate(data = map2(file, sheet, read_sisetukijun_xlsx)) %>% 
  unnest(cols = c(data)) 

#ダウンロードしたファイルを読み込む準備
df_file <- dplyr::tibble(file = files) %>% 
  mutate(get_date = str_extract(file, 'output/[0-9]{8}')) %>% 
  mutate(get_date = str_extract(get_date, '[0-9]{8}')) %>% 
  mutate(get_date = ymd(get_date)) %>% 
  mutate(sheet = map(file, openxlsx::getSheetNames)) %>%
  unnest(cols = c(sheet)) 

# ファイル名から厚生局を取得
df_file <- df_file %>% 
  mutate(厚生局 = str_replace(file,original_dir,'')) %>% 
  mutate(厚生局 = str_replace(厚生局,'^/','')) %>% #先頭の/を削除
  mutate(厚生局 = str_replace(厚生局,'/.*','')) %>% # /以降を削除
  print()

# データの読み込み
df_file <- df_file %>% 
  mutate(data = map2(file, sheet, read_sisetukijun_xlsx)) 

# dataにdate列を追加
df_file <- df_file %>% 
  mutate(data = map2(data, get_date, ~mutate(.x, get_date = .y))) 

# dataに厚生局列を追加
df_file <- df_file %>% 
  mutate(data = map2(data, 厚生局, ~mutate(.x, 厚生局 = .y))) 

# データを結合
df_all <- df_file %>% 
  select(data) %>% 
  unnest(cols = c(data)) 

df_all %>% glimpse()

# 算定開始日の不正データを発見→厚生局に問い合わせをしたら2024/7/1版で修正するとのこと。
# 2024/6/1版は手直しするしかなくなった。
df_all <- df_all %>% 
  mutate(算定開始年月日 = case_when(
    get_date <= ymd(20240831) & str_detect(算定開始年月日, "令和30年 6月 1日") ~ "令和 6年 6月 1日"
    ,T  ~ 算定開始年月日
  )) 

################################################################################

# 算定開始年月日を西暦に変換
# df_all <- df_all %>% 
#   mutate(西暦算定開始年月日 = zipangu::convert_jdate(str_replace_all(算定開始年月日, " ", "")),.after=算定開始年月日) 

################################################################################

# 西暦変換は時間がかかるので、一旦ユニークを取得してから変換しjoinする

# 算定開始年月日のスペース削除
df_all <- df_all %>% 
  mutate(算定開始年月日 = str_replace_all(算定開始年月日, " ", "")) 

# 算定開始年月日のユニークを取得して西暦変換  
santei_kaisi_mst <- df_all %>% 
  distinct(算定開始年月日) %>%
  mutate(西暦算定開始年月日 = zipangu::convert_jdate(算定開始年月日)) %>% 
  print()

# 結合
df_all <- df_all %>% 
  left_join(santei_kaisi_mst, by='算定開始年月日') %>%
  glimpse()

# # 西暦変換によるNAが発生していないことを確認
# df_all %>%
#   filter(is.na(西暦算定開始年月日),!is.na(算定開始年月日))

################################################################################

# 医科以外のデータを削除
df_all <- df_all %>% 
  filter(区分=='医科')

################################################################################

# 医療機関コードを作成
df_all <- df_all %>% 
  mutate(医療機関コード = str_glue('{都道府県コード}1{医療機関番号}'),.before=都道府県コード) 

# 併設医療機関コード列を作成
df_all <- df_all %>% 
  mutate(併設医療機関コード = case_when(
    is.na(併設医療機関番号) ~ ''
    ,TRUE ~ str_glue('{都道府県コード}1{併設医療機関番号}')
  ),.before=都道府県コード) %>% 
  print()

################################################################################

# 種別の穴埋め (中国以外はすべてNAであった。消すわけにいかないので未分類としておく)
df_all <- df_all %>% 
  replace_na(list(種別コード='99',種別='未分類')) %>%
  print()

################################################################################

# # 県ごとに最大の算定開始年月日を計算して出力
# pref_update_date <- df_all %>% 
#   group_by(都道府県コード,都道府県名) %>%
#   summarise(update_date = max(西暦算定開始年月日,na.rm = T)) %>%
#   ungroup() 
# 
# # Excelで書き出し
# pref_update_date %>% 
#   writexl::write_xlsx(str_glue('{output_dir}/pref_update_date.xlsx'))

################################################################################

# # 厚生局ごとに最大の算定開始年月日を計算して出力
kouseikyoku_update_date <- df_all %>%
  group_by(厚生局) %>%
  summarise(update_date = max(西暦算定開始年月日,na.rm = T)) %>%
  ungroup()

# # Excelで書き出し
kouseikyoku_update_date %>%
  writexl::write_xlsx(str_glue('{output_dir}/kouseikyoku_update_date.xlsx'))

################################################################################

# df_allにupdate_dateを追加
df_all <- df_all %>% 
  left_join(kouseikyoku_update_date, by = c('厚生局')) 

# 生成物をparquet出力
title <- str_glue('{output_dir}/df_all.parquet')
df_all %>% write_parquet(title)
message(str_glue('出力完了: {title}'))

################################################################################

print(data.frame(kouseikyoku_update_date))

message('01.1_make_df_all_parquet.Rが終了しました')
