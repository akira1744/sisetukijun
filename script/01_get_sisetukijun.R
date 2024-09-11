rm(list=ls())

pacman::p_load(
  rvest
  ,here
  ,httr
  ,openxlsx
  ,zipangu
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

#東北、関東信越、近畿、中国、四国
df_kouseikyoku <- dplyr::tibble(
  district = c("東北", "関東信越", "四国", "近畿", "中国"),
  url = c(
    "https://kouseikyoku.mhlw.go.jp/tohoku/gyomu/gyomu/hoken_kikan/documents/201805koushin.html",
    "https://kouseikyoku.mhlw.go.jp/kantoshinetsu/chousa/kijyun.html",
    "https://kouseikyoku.mhlw.go.jp/shikoku/gyomu/gyomu/hoken_kikan/shitei/index.html",
    "https://kouseikyoku.mhlw.go.jp/kinki/gyomu/gyomu/hoken_kikan/shitei_jokyo_00004.html",
    "https://kouseikyoku.mhlw.go.jp/chugokushikoku/chousaka/shisetsukijunjuri.html"),
  contains_jp = c("Excel（ZIP）", "医科（ZIP）", "（ZIP）", "（ZIP）", "各県全体（ZIP）")) |> 
    mutate(a_con = str_c("a:contains('", contains_jp, "')")) %>% 
  glimpse()

#東北、関東信越、近畿、中国、四国の関数
get_sisetukijun1<- function(district, url, a_contains){
  
  output_dir1 <- str_glue('{original_dir}/{district}') 
  
  if (!dir.exists(output_dir1)) {
    dir.create(output_dir1)
  }
  
  # ウェブページを読み込み、必要なリンクを抽出します
  web_page <- read_html(url)
  links <- web_page %>% html_nodes(a_contains) %>% html_attr("href")
  
  # ベースURLを定義します
  base_url <- "https://kouseikyoku.mhlw.go.jp"
  
  # リンクの完全なURLを生成します
  full_links <- paste0(base_url, links)
  
  # 各リンク先ファイルのサイズを取得します
  get_file_size <- function(url) {
    response <- HEAD(url)
    as.numeric(headers(response)["content-length"])
  }
  
  file_sizes <- sapply(full_links, get_file_size)
  
  # 最大サイズのファイルのリンクを取得します
  max_index <- which.max(file_sizes)
  max_file_url <- full_links[max_index]
  
  # ダウンロードディレクトリを設定します
  download_dir <- "downloads"
  if (!dir.exists(download_dir)) {
    dir.create(download_dir)
  }
  
  # 最大サイズのファイルをダウンロードします
  zip_filename <- basename(max_file_url)
  zip_filepath <- file.path(download_dir, zip_filename)
  download.file(max_file_url, zip_filepath)
  
  # ダウンロードされたファイルを解凍する
  unzip_dir <- tempdir()
  
  # unzip_dirの中身を削除
  unlink(unzip_dir, recursive = TRUE)
  
  # システムコマンドを使って解凍します
  zip_command <- paste("unzip", shQuote(zip_filepath), "-d", shQuote(unzip_dir))
  system(zip_command)
  
  # 元のディレクトリに保存するためのパスを確認します
  # original_dir <- dirname(download_dir)
  
  # 解凍されたフォルダ内のエクセルファイルを検索し、元のディレクトリにコピーします
  unzip_subdirs <- list.dirs(unzip_dir, recursive = TRUE)
  
  for (subdir in unzip_subdirs) {
    excel_files <- list.files(subdir, pattern = "\\.xlsx$", full.names = TRUE)
    excel_files
    if (length(excel_files) > 0) {
      file.copy(excel_files, output_dir1, overwrite = TRUE)
    }
  }
  
  #downloadディレクトリを削除します
  unlink(download_dir, recursive = TRUE)
}

#東北、関東信越、近畿、中国、四国の取得
for(i in 1:nrow(df_kouseikyoku)){
  # ターゲットURL
  district <- df_kouseikyoku$district[i]
  url <- df_kouseikyoku$url[i]
  a_contains <- df_kouseikyoku$a_con[i]
  
  get_sisetukijun1(district,url, a_contains)
}

################################################################################

#北海道
url <- "https://kouseikyoku.mhlw.go.jp/hokkaido/gyomu/gyomu/hoken_kikan/todokede_juri_ichiran.html"

# ウェブページのHTMLを読み込む
webpage <- read_html(url)

# エクセルファイルへのリンクを取得
links <- webpage %>%
  html_nodes("a") %>%
  html_attr("href")

# エクセルファイルのリンクを特定
excel_links <- links[grepl("\\.xlsx$", links)]

# エクセルファイルが存在するか確認
if (length(excel_links) == 0) {
  stop("エクセルファイルのリンクが見つかりませんでした。")
}

# ダウンロードしたファイル名を格納するリスト
downloaded_files <- c()

# ベースURLとファイルリンクを結合し、すべてのエクセルファイルをダウンロード
for (link in excel_links) {
  # 絶対URLに変換
  download_link <- paste0("https://kouseikyoku.mhlw.go.jp", link)
  
  # ファイル名を抽出
  file_name <- basename(link)
  
  # ファイルをダウンロードして保存
  GET(download_link, write_disk(file_name, overwrite = TRUE))
  cat(file_name, "のダウンロードが完了しました。\n")
  
  # ダウンロードしたファイル名をリストに追加
  downloaded_files <- c(downloaded_files, file_name)
}

# ファイルサイズを取得して最大のファイルを特定
file_sizes <- sapply(downloaded_files, file.size)
largest_file <- downloaded_files[which.max(file_sizes)]

cat("最大ファイルは:", largest_file, "です。\n")

output_dir2 <- str_glue('{original_dir}/北海道')
if (!dir.exists(output_dir2)) {
  dir.create(output_dir2)
}

file.copy(largest_file, output_dir2, overwrite = TRUE)

# 他のファイルを削除
# files_to_delete <- setdiff(downloaded_files, largest_file)
sapply(downloaded_files, file.remove)

################################################################################

#東海北陸
# ダウンロード対象ページのURL
url <- "https://kouseikyoku.mhlw.go.jp/tokaihokuriku/newpage_00349.html"

# ウェブページを読み込み
webpage <- read_html(url)

# 対象リンクを特定
links <- webpage %>%
  html_nodes("a") %>%
  .[str_detect(html_text(.), "^届出受理医療機関名簿（医科）")] %>%
  html_attr("href")

# 最初のリンクを取得
first_link <- links[1]

# 完全なURLを作成
file_url <- paste0("https://kouseikyoku.mhlw.go.jp", first_link)

# ダウンロードディレクトリを設定します
download_dir <- "downloads"
if (!dir.exists(download_dir)) {
  dir.create(download_dir)
}

# 該当ファイルをダウンロードします
zip_filename <- basename(file_url)
zip_filepath <- file.path(download_dir, zip_filename)
download.file(file_url, zip_filepath)

# ファイル名を指定してダウンロード
# output_file <- "downloaded_file.zip"
# GET(file_url, write_disk(output_file, overwrite = TRUE))

# ダウンロードされたファイルを解凍する
unzip_dir <- tempdir() %>% print()

# unzip_dirの中身を削除
unlink(unzip_dir, recursive = TRUE)

# システムコマンドを使って解凍します
zip_command <- paste("unzip", shQuote(zip_filepath), "-d", shQuote(unzip_dir))
system(zip_command)

# 元のディレクトリに保存するためのパスを確認します
# original_dir <- dirname(download_dir)

# 解凍されたフォルダ内のエクセルファイルを検索し、元のディレクトリにコピーします
unzip_subdirs <- list.dirs(unzip_dir, recursive = TRUE) %>% print()

output_dir3 <- str_glue('{original_dir}/東海北陸')
if (!dir.exists(output_dir3)) {
  dir.create(output_dir3)
}

for (subdir in unzip_subdirs) {
  excel_files <- list.files(subdir, pattern = "\\.xls(x)?$", full.names = TRUE)
  if (length(excel_files) > 0) {
    file.copy(excel_files, output_dir3, overwrite = TRUE)
  }
}

#downloadディレクトリを削除します
unlink(download_dir, recursive = TRUE)


################################################################################

#九州
# ターゲットURL
url <- "https://kouseikyoku.mhlw.go.jp/kyushu/gyomu/gyomu/hoken_kikan/index_00007.html"

# ウェブページを読み込み、必要なリンクを抽出します
web_page <- read_html(url)
links <- web_page %>% html_nodes("a:contains('エクセルデータ（ZIP）')") %>% html_attr("href")

# 最新ファイル判定のための正規表現を作成
latest_prefix <- max(gsub("^.*/([^_]+_[0-9]+)_.*$", "\\1", links))

latest_prefix

# ダウンロードディレクトリを設定します
download_dir <- "downloads"
if (!dir.exists(download_dir)) {
  dir.create(download_dir)
}

# 最新のリンクのみを抽出してダウンロード
latest_links <- links[grepl(paste0(latest_prefix), links)]
  
for (link in latest_links) {
  zip_url <- paste0("https://kouseikyoku.mhlw.go.jp", link)
  zip_filename <- basename(zip_url)
  zip_filepath <- file.path(download_dir, zip_filename)
  download.file(zip_url, zip_filepath)
}

list.files("./downloads")

# 解凍用ディレクトリを設定します
unzip_dir <- "unzipped"
if (!dir.exists(unzip_dir)) {
  dir.create(unzip_dir)
}

# 解凍したファイルをフィルタリングして移動します
target_dir <- "target_files"
if (!dir.exists(target_dir)) {
  dir.create(target_dir)
}

# 出力用フォルダを作成
output_dir4 <- str_glue('{original_dir}/九州')
if (!dir.exists(output_dir4)) {
  dir.create(output_dir4)
}

# ダウンロードしたZIPファイルを解凍し、条件に合うファイルを移動します
zip_files <- list.files(download_dir, pattern = "\\.zip$", full.names = TRUE)
for (zip_file in zip_files) {
  unzip(zip_file, exdir = unzip_dir)
  files <- list.files(unzip_dir,
     pattern = str_c(latest_prefix, ".*_ika_.*\\.xlsx$"), full.names = TRUE)
  if (length(files) > 0) {
    file.copy(files, output_dir4, overwrite = TRUE) 
  }
  # 解凍したファイルを削除
  unlink(list.files(unzip_dir, full.names = TRUE, recursive = TRUE))
}

# 使用したディレクトリを削除します
unlink(download_dir, recursive = TRUE)
unlink(unzip_dir, recursive = TRUE)
unlink(target_dir, recursive = TRUE)

################################################################################

message('01_get_sisetukijun.Rが終了しました')
