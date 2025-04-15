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
original_dir <- here(str_glue('{output_dir}/original')) %>% print()

# 書き込み権限を変更
system(paste("sudo chmod -R 777", output_dir))

# 初期化
if (dir.exists(output_dir)){
  # output_dirを一旦削除して初期化
  unlink(output_dir, recursive = TRUE)
}

# original_dirをrecursiveで作成
if (!dir.exists(original_dir)) {
  dir.create(original_dir,recursive = TRUE)
}
################################################################################

# 東海北陸と中国の更新日形式: 2024年11月8日
# その他ページの更新日形式: 更新日：2024年11月11日
get_kousin_date <- function(url,district){
  
  date_pattern <- "\\d{4}年\\d{1,2}月\\d{1,2}日" 
  
  # ulrからpタグのテキストを取得
  texts <- read_html(url) %>% 
    html_nodes("p") %>% 
    html_text()
  
  # 東海北陸と中国の場合は最初に出てきた日付形式のものを抽出
  if(district %in% c('北海道','東北',"東海北陸",'四国',"中国")){
    kousin_date <-  texts %>% 
      str_subset(date_pattern) %>% 
      str_extract(date_pattern) %>%
      head(1) 
    
  }else if(district == '近畿'){
    # 令和6年12月4日
    gengo_date_pattern <- "令和\\d{1,2}年\\d{1,2}月\\d{1,2}日"
    kousin_date <-  texts %>% 
      str_subset(gengo_date_pattern) %>% 
      str_extract(gengo_date_pattern) %>% 
      head(1) %>% 
      convert_jdate()
    
  }else{
    # その他地区の場合は「更新日」が含まれるテキストを抽出
    kousin_date <- texts %>% 
      str_subset("更新日") %>% 
      str_subset(date_pattern) %>% 
      str_extract(date_pattern) %>% 
      head(1)
  }
  
  # 見つからなかった場合のデフォルト値を設定
  if (length(kousin_date) == 0) {
    update_date <- NA_character_
  }
  
  return(kousin_date)
}

################################################################################

# 北海道: 【令和6年11月1日現在】
# 東北:【令和6年10月1日現在】
# 関東信越:令和6年10月1日現在 
# 東海北陸: 令和6年11月1日現在
# 四国: （令和6年11月1日現在） 表の中にある!
# 近畿: 令和6年11月1日現在
# 中国: （令和6年10月1日現在）
# 九州: 令和6年11月1日現在
get_update_date <- function(url,district){
  
  pattern <- "令和\\d{1,2}年\\d{1,2}月\\d{1,2}日現在" 
  
  update_date <- read_html(url) %>% 
    html_text() %>% 
    str_extract(pattern)
  
  # 見つからなかった場合のデフォルト値を設定
  if (length(update_date) == 0) {
    update_date <- NA_character_
  }
  
  return(update_date)
}

################################################################################

# 一覧
urls <- dplyr::tibble(
  kousei = c('北海道',"東北", "関東信越",'東海北陸', "四国", "近畿", "中国","九州"),
  url = c(
    "https://kouseikyoku.mhlw.go.jp/hokkaido/gyomu/gyomu/hoken_kikan/todokede_juri_ichiran.html",
    "https://kouseikyoku.mhlw.go.jp/tohoku/gyomu/gyomu/hoken_kikan/documents/201805koushin.html",
    "https://kouseikyoku.mhlw.go.jp/kantoshinetsu/chousa/kijyun.html",
    "https://kouseikyoku.mhlw.go.jp/tokaihokuriku/newpage_00349.html",
    "https://kouseikyoku.mhlw.go.jp/shikoku/gyomu/gyomu/hoken_kikan/shitei/index.html",
    "https://kouseikyoku.mhlw.go.jp/kinki/gyomu/gyomu/hoken_kikan/shitei_jokyo_00004.html",
    "https://kouseikyoku.mhlw.go.jp/chugokushikoku/chousaka/shisetsukijunjuri.html",
    "https://kouseikyoku.mhlw.go.jp/kyushu/gyomu/gyomu/hoken_kikan/index_00007.html"
  )
) 

# get_kousin_date("https://kouseikyoku.mhlw.go.jp/hokkaido/gyomu/gyomu/hoken_kikan/todokede_juri_ichiran.html","北海道")
# get_kousin_date("https://kouseikyoku.mhlw.go.jp/tohoku/gyomu/gyomu/hoken_kikan/documents/201805koushin.html","東北")
# get_kousin_date("https://kouseikyoku.mhlw.go.jp/kantoshinetsu/chousa/kijyun.html","関東信越")
# get_kousin_date("https://kouseikyoku.mhlw.go.jp/tokaihokuriku/newpage_00349.html","東海北陸")
# get_kousin_date("https://kouseikyoku.mhlw.go.jp/shikoku/gyomu/gyomu/hoken_kikan/shitei/index.html","四国")
# get_kousin_date("https://kouseikyoku.mhlw.go.jp/kinki/gyomu/gyomu/hoken_kikan/shitei_jokyo_00004.html","近畿")
# get_kousin_date("https://kouseikyoku.mhlw.go.jp/chugokushikoku/chousaka/shisetsukijunjuri.html","中国")
# get_kousin_date("https://kouseikyoku.mhlw.go.jp/kyushu/gyomu/gyomu/hoken_kikan/index_00007.html","九州")
# 

# kousin_dateを取得
urls <- urls %>% 
  mutate(kousin_date = map2_chr(url,kousei,~get_kousin_date(.x,.y))) 

# urls %>% 
#   filter(is.na(kousin_date)) 

# update_dateを取得
urls <- urls %>% 
  mutate(update_date = map_chr(url,get_update_date)) 

# 現在時刻の付与
now <- Sys.time()

urls <- urls %>%
  mutate(date = ymd(Sys.Date())) %>% 
  mutate(time  = str_glue("{hour(now)}{minute(now)}")) 
  
# 列整理
urls <- urls %>% 
  select(date,time,kousei,kousin_date,update_date) 


# データの保存
urls %>% 
  write_csv(str_glue("{output_dir}/dates.csv"))

################################################################################

# # データフレームをlong形式に変換
# long_data <- urls %>%
#   pivot_longer(
#     cols = c(kousin_date, update_date), 
#     names_to = "type", 
#     values_to = "value"
#   ) %>%
#   mutate(column_name = str_glue("{kousei}_{type}")) # 列名を動的に作成
# 
# long_data
# 
# # 必要な形式に変換
# wide_data <- long_data %>%
#   select(column_name, value) %>%
#   pivot_wider(
#     names_from = column_name, 
#     values_from = value
#   )
# 
# # 結果の確認
# wide_data
# 
# now <- ymd_hms(Sys.time())
# 
# now
# 
# wide_data <- wide_data %>% 
#   mutate(date = as.Date(now),.before=1) %>% 
#   mutate(time  = str_glue("{hour(now)}{minute(now)}"),.after=date) %>%
#   print()
# 
# 
# # csvに追記
# write_csv(wide_data, "output/dates.csv")
# write_csv(wide_data, "output/dates.csv", append = TRUE)
