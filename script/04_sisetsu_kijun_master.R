rm(list=ls())

pacman::p_load(
  here
  ,httr
  ,rvest
  ,stringi
  ,DBI
  ,writexl
  ,lubridate
  ,duckdb
  ,duckplyr
  ,arrow
  ,tidyverse
  ,tidylog
)

# 対象ページ2つ
# 関東信越厚生局の基本診療料の届出一覧（令和6年度診療報酬改定）
# 関東信越厚生局の特掲診療料の届出一覧（令和6年度診療報酬改定）
urls <- c(
  "https://kouseikyoku.mhlw.go.jp/kantoshinetsu/shinsei/shido_kansa/shitei_kijun/kihon_shinryo_r06.html"
  ,"https://kouseikyoku.mhlw.go.jp/kantoshinetsu/shinsei/shido_kansa/shitei_kijun/tokukei_shinryo_r06.html"
)

urls <- tribble(
  ~source, ~url,
  '基本診療料', "https://kouseikyoku.mhlw.go.jp/kantoshinetsu/shinsei/shido_kansa/shitei_kijun/kihon_shinryo_r06.html",
  '特掲診療料', "https://kouseikyoku.mhlw.go.jp/kantoshinetsu/shinsei/shido_kansa/shitei_kijun/tokukei_shinryo_r06.html",
) %>% 
  print()
  
# urlからtableを抽出するための関数
read_sisetukijun_mst_from_url<- function(url){
  
  # SSLエラーを回避するための設定
  set_config(config(ssl_verifypeer = 0L))
  
  # urlからpageを読み込み
  webpage <- rvest::read_html(url)
  
  # classがdatatableのすべてのテーブルを取得
  tables <- webpage %>% 
    rvest::html_nodes("table.datatable") %>%
    rvest::html_table()
  
  # 1つのurlにtableが2つあるので2つ目を使う
  table <- tables[[2]]
  
  # 1列目と2列目だけを使う
  table <- table %>% 
    select(1, 2) %>% 
    rename(num = 1, item = 2) 
    
  # 1,2行目を削除
  table <- table %>% 
    slice(-1, -2) 
  
  # numとitemが同じものが複数取れてしまうので重複削除
  table <- table %>% 
    distinct(num,item)
  
  return(table)
}


# urlを関数に渡して必要なtableを読み込み
urls <- urls %>% 
  mutate(data = map(url,read_sisetukijun_mst_from_url)) %>% 
  print()

# 基本診療料と,特掲診療料を縦に結合
df <- urls %>% 
  select(source, data) %>% 
  unnest(cols = c(data)) %>% 
  print()

################################################################################

# 結合できるか確認

################################################################################

# すべてのrdsを格納するdb
all_db_path <- 'sisetukijun_all.duckdb'

# connect
all_db_con <- dbConnect(duckdb(),all_db_path,read_only=FALSE)

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) %>% print()

all_jurikigou <- tbl(all_db_con,'sisetukijun_all_update_date') %>% 
  distinct(受理記号,受理届出名称) %>% 
  collect() %>% 
  print()

# 受理記号が同じで、名称が異なるものがあったが、意味合いとしては同じとして扱ってよさそう
all_jurikigou %>% 
  group_by(受理記号) %>% 
  filter(n()>1)

# 受理記号が同じで、名称が異なるものはupdate_dateが最新の方を残すことにする
all_jurikigou <- tbl(all_db_con,'sisetukijun_all_update_date') %>% 
  filter(受理届出名称!='') %>% 
  distinct(受理記号,受理届出名称,update_date) %>% 
  collect() %>% 
  arrange(desc(update_date)) %>% 
  distinct(受理記号,受理届出名称) %>% 
  arrange(受理記号) %>% 
  rowid_to_column('jurikigou_id') %>%
  print()

################################################################################

# normalize

################################################################################

# ローマ数字をアラビア数字に変換する関数を定義
roman_to_arabic <- function(s) {
  # ローマ数字とアラビア数字のマッピング
  mapping <- list('Ⅰ' = '1', 'Ⅱ' = '2', 'Ⅲ' = '3', 'Ⅳ' = '4',
                  'Ⅴ' = '5', 'Ⅵ' = '6', 'Ⅶ' = '7', 'Ⅷ' = '8',
                  'Ⅸ' = '9', 'Ⅹ' = '10', 'Ⅺ' = '11', 'Ⅻ' = '12')

  # マッピングを使用してローマ数字をアラビア数字に置換
  for (roman in names(mapping)) {
    s <- stri_replace_all_fixed(s, roman, mapping[[roman]], vectorize_all = FALSE)
  }
  return(s)
}

# 半角のローマ数字をアラビア数字に変換する関数を定義
roman_to_arabic_half <- function(s) {
  # ローマ数字とアラビア数字のマッピング
  mapping <- list('I' = '1', 'II' = '2', 'III' = '3', 'IV' = '4',
                  'V' = '5', 'VI' = '6', 'VII' = '7', 'VIII' = '8',
                  'IX' = '9', 'X' = '10', 'XI' = '11', 'XII' = '12')
  
  # マッピングを使用してローマ数字をアラビア数字に置換
  for (roman in names(mapping)) {
    s <- stri_replace_all_fixed(s, roman, mapping[[roman]], vectorize_all = FALSE)
  }
  return(s)
}

################################################################################

# ホームページのitem列をnormalize
df <- df %>% 
  mutate(item_norm = stri_trans_general(item, "Halfwidth-Fullwidth")) %>% 
  mutate(item_norm = roman_to_arabic(item_norm)) %>% # ローマ数字をアラビア数字に変換
  mutate(item_norm = stri_trans_nfkc(item_norm)) %>% # NFKCで正規化
  mutate(item_norm = roman_to_arabic_half(item_norm)) %>% # ローマ数字をアラビア数字に変換
  mutate(num = if_else(num=='',NA_character_,num)) %>% # numが空白の場合はNAに変換
  arrange(num,item_norm) %>% 
  distinct(item_norm,.keep_all=T) %>% 
  print()

# エクセルの受理記号列をnormalize
all_jurikigou <- all_jurikigou %>% 
  mutate(受理記号_norm = stri_trans_general(受理記号, "Halfwidth-Fullwidth")) %>% 
  mutate(受理記号_norm = roman_to_arabic(受理記号_norm)) %>%
  mutate(受理記号_norm = stri_trans_nfkc(受理記号_norm)) %>% 
  mutate(受理記号_norm = roman_to_arabic_half(受理記号_norm)) %>%
  print()

################################################################################

# 大カテゴリでしか番号が振られていないものがあるので,カテゴリ列を作成

################################################################################

# エクセルの受理記号_normを変形し,末尾の数字を削除してcategory列を作成する
all_jurikigou <- all_jurikigou %>% 
  mutate(受理記号_category = str_replace_all(受理記号_norm, '[0-9]+$', '')) %>% 
  mutate(受理希望_category_no = str_extract(受理記号_norm, '[0-9]+$')) %>%
  print()

# ホームページのitem_norm列を変形し、末尾の数字を削除してcategory列を作成する
df_category <- df %>% 
  mutate(item_category = str_replace_all(item_norm, '[0-9]+$', '')) %>%
  mutate(num = if_else(num=='',NA_character_,num)) %>% # ''が上に来てしまうので、欠損に変換
  arrange(source,num,item_norm,item) %>% 
  distinct(source,num,item_category,.keep_all=T) %>% 
  rename(num_category=num) %>%
  select(num_category,item_category) %>% 
  print()

################################################################################

# チェック用
df %>% 
  filter(str_detect(item,'先'))

all_jurikigou %>%
  filter(受理記号=='こ連指Ⅰ')

################################################################################

# join
sisetukijun_match <- all_jurikigou %>% 
  full_join(df,by=c('受理記号_norm'='item_norm')) 

# この時点ではjurikigou_idがユニークであることを確認
sisetukijun_match %>% 
  filter(is.na(num)) %>% 
  group_by(jurikigou_id) %>% 
  filter(n()>1)

# categoryでjoin
sisetukijun_match2 <- sisetukijun_match %>% 
  full_join(df_category,by=c('受理記号_category'='item_category')) %>% 
  print()

# num_category_noを作成
sisetukijun_match2 <- sisetukijun_match2 %>% 
  mutate(num_category_no = case_when(
    is.na(受理希望_category_no) ~ NA_character_
    ,is.na(num_category) ~ NA_character_
    ,TRUE ~ str_glue('{num_category}-{受理希望_category_no}')
  )) %>% 
  print()

# 採用numを決定してから,jurikigou_idでdistinctする
sisetukijun_match3 <- sisetukijun_match2 %>% 
  mutate(num_match = if_else(!is.na(num),num,num_category_no)) %>%
  arrange(jurikigou_id,num_match) %>%
  distinct(jurikigou_id,.keep_all=T) %>%
  print()

# 受理記号が先ー〇〇のものと先〇〇のものは,num_matchを1000-〇〇にする
sisetukijun_match4 <- sisetukijun_match3 %>% 
  mutate(num_match = case_when(
    !is.na(num_match) ~ num_match
    ,str_detect(受理記号_norm,'^先-') ~ str_replace(受理記号_norm,'^先-','3-')
    ,str_detect(受理記号_norm,'^先') ~ str_replace(受理記号_norm,'^先','3-')
    ,TRUE ~ num_match
  )) %>% 
  print()

# チェック
sisetukijun_match4 %>% 
  filter(is.na(num_match)) %>% 
  print()

################################################################################

# num_matchがNAのものは9-から始まる連番をふっておく
sisetukijun_match5 <- sisetukijun_match4 %>% 
  group_by(num_match) %>% 
  mutate(hokan_rank = dense_rank(jurikigou_id)) %>% 
  ungroup() %>%
  mutate(num_hokan = str_glue('9-{hokan_rank}')) %>%
  mutate(num_final = if_else(!is.na(num_match),num_match,num_hokan)) %>%
  select(-num_hokan) %>% 
  glimpse()

################################################################################

# 出力ように整理
sisetukijun_match6 <- sisetukijun_match5 %>% 
  select(num_final,受理記号,受理届出名称) %>% 
  arrange(num_final) %>% 
  print()

# num_finalを-を境にして3つに分割
sisetukijun_match7 <- sisetukijun_match6 %>% 
  mutate(num_final1 = str_extract(num_final,'^[0-9]+')) %>% 
  mutate(num_final2 = str_extract(num_final,'-[0-9]+-')) %>% 
  mutate(num_final2 = str_replace_all(num_final2,'-','')) %>%
  mutate(num_final3 = str_extract(num_final,'[0-9]+$')) %>% 
  print()

# num_final2を0つめにする
sisetukijun_match8 <- sisetukijun_match7 %>% 
  mutate(num_final2 = case_when(
    is.na(num_final2) ~ NA_character_
    ,TRUE ~ str_pad(num_final2,3,pad='0')
  )) %>% 
  print()

# num_final3を0つめにする
sisetukijun_match9 <- sisetukijun_match8 %>% 
  mutate(num_final3 = case_when(
    is.na(num_final3) ~ NA_character_
    ,TRUE ~ str_pad(num_final3,3,pad='0')
  )) %>% 
  print()

# 整理番号として結合
sisetukijun_match10 <- sisetukijun_match9 %>% 
  mutate(整理番号 = case_when(
    is.na(num_final2) & is.na(num_final3) ~ num_final1
    ,is.na(num_final2) ~ str_glue('{num_final1}-{num_final3}')
    ,is.na(num_final3) ~ str_glue('{num_final1}-{num_final2}')
    ,TRUE ~ str_glue('{num_final1}-{num_final2}-{num_final3}')
  )) %>%
  print()


# 整理
sisetukijun_match10 <- sisetukijun_match10 %>% 
  filter(!is.na(受理記号)) %>% 
  select(整理番号,受理記号,受理届出名称) %>% 
  arrange(整理番号) %>% 
  print()

# 確認用に出力
list(
  '施設基準match'=sisetukijun_match10
  ,'施設基準Excelのマスタ'=all_jurikigou
  ,'施設基準HP'=df
  )%>% 
  write_xlsx('tmp/sisetukijun_match.xlsx')

################################################################################

sisetukijun_match10 %>% 
  print()

nasi_table <- tibble(
  整理番号='9-999'
  ,受理記号='なし'
  ,受理届出名称='なし'
  )

mst_todokede_no <- rbind(nasi_table,sisetukijun_match10) %>% 
  print()

# すべてのrdsを格納するdb
db_path <- 'sisetukijun.duckdb'

# connect
con <- dbConnect(duckdb(),db_path,read_only=FALSE)

# table一覧を確認
DBI::dbListTables(con) %>% print()

mst_todokede <- tbl(con,'mst_todokede') %>% 
  collect() %>% 
  print()

mst_todokede <- mst_todokede %>% 
  left_join(mst_todokede_no,by=c('受理届出名称')) %>% 
  print()

mst_todokede <- mst_todokede %>% 
  select(受理届出コード,整理番号,受理記号,受理届出名称) %>% 
  print()

# mst_todokedeをoverwrite
dbWriteTable(con,'mst_todokede',mst_todokede,overwrite=T)

################################################################################

DBI::dbDisconnect(con)
DBI::dbDisconnect(all_db_con)

################################################################################

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

################################################################################
