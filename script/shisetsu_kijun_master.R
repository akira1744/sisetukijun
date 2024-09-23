# library(conflicted)
# library(tidyverse)
# library(stringi)
# library(rvest)
# library(httr)

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
  mutate(data = map(url,read_sisetukijun_mst_from_url)) 

# 基本診療料と,特掲診療料を縦に結合
df <- urls %>% 
  select(source, data) %>% 
  unnest(cols = c(data)) %>% 
  print()

df %>% 
  filter(str_detect(item,'DX'))

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
  arrange(desc(update_date)) %>% 
  select(受理記号,受理届出名称) %>% 
  filter(受理届出名称!='') %>% 
  distinct(受理記号,.keep_all=T) %>% 
  collect() %>% 
  print()

################################################################################

# 表記ブレがあるようだ

all_jurikigou %>% 
  filter(str_detect(受理記号,'DX|ＤＸ'))

# 半角を全角へ
df <- df %>% 
  mutate(item = stri_trans_general(item, "Halfwidth-Fullwidth"))

all_jurikigou <- all_jurikigou %>% 
  mutate(受理記号 = stri_trans_general(受理記号, "Halfwidth-Fullwidth")) %>% 
  print()

# NFKDで正規化
df <- df %>% 
  mutate(item_nfkc = stri_trans_nfkc(item)) 
  
all_jurikigou <- all_jurikigou %>% 
  mutate(受理記号_nfkc = stri_trans_nfkc(受理記号),.after=受理記号) %>% 
  print()

sisetukijun_match <- df %>% 
  full_join(all_jurikigou,by=c('item_nfkc'='受理記号_nfkc')) %>% 
  arrange(item_nfkc)

list(
  '施設基準match'=sisetukijun_match
  ,'施設基準Excelのマスタ'=all_jurikigou
  ,'施設基準HP'=df
  )%>% 
  write_xlsx('tmp/sisetukijun_match.xlsx')


;    

sisetukijun_match %>% 
  filter(is.na(num))

# ローマ数字をアラビア数字に変換する関数
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

# 使用例
print(roman_to_arabic('心リハⅠ'))  # 出力: 心リハ1
print(roman_to_arabic('心リハⅡ'))  # 出力: 心リハ2
