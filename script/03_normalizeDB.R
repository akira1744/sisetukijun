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

output_dir <- here('output') %>% print()

################################################################################

# すべてのrdsを格納するdb
all_db_path <- 'sisetukijun_all.duckdb'

# connect
all_db_con <- dbConnect(duckdb(),all_db_path,read_only=FALSE)

# table一覧を確認
all_db_tables <- DBI::dbListTables(all_db_con) %>% print()

################################################################################

# normalize後のdataを格納するdb
db_path <- 'sisetukijun.duckdb'

con <- dbConnect(duckdb(),db_path,read_only=FALSE)

# 書き込み権限を変更
system(paste("sudo chmod -R 666", db_path))

# table一覧を確認
db_tables <- DBI::dbListTables(con) %>% print()

################################################################################

# sisetuテーブルが存在したらdropする
DBI::dbExecute(con,'DROP TABLE IF EXISTS sisetu_main;')
DBI::dbExecute(con,'DROP TABLE IF EXISTS sisetu_sub;')

# todokedeテーブルが存在したらdropする
DBI::dbExecute(con,'DROP TABLE IF EXISTS todokede;')

################################################################################

# サンプル確認
sample <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% head() %>% collect() %>% glimpse()
sample %>% names() %>% dput()

################################################################################

# update_dateのマスタを作成する
mst_update_date <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
  distinct(update_date) %>% 
  collect() %>% 
  arrange(update_date) %>%
  print()

# DBにmst_update_dateを書き込み
DBI::dbWriteTable(con, 'mst_update_date', mst_update_date, overwrite=T) %>% print()

################################################################################

# 厚生局マスタ
mst_kouseikyoku <- tibble(
  厚生局コード = c("1", "2", "3", "4", "5", "6", "7", "8"),
  厚生局 = c("北海道", "東北", "関東信越","東海北陸","近畿" , "中国", "四国", "九州")  
) %>% print()

# DBにmst_kouseikyokuを書き込み
DBI::dbWriteTable(con, 'mst_kouseikyoku', mst_kouseikyoku, overwrite=T) %>% print()

################################################################################

# 都道府県一覧を作成
mst_pref <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
  distinct(厚生局,都道府県コード,都道府県名) %>% 
  collect() %>% 
  print()

# 厚生局cdを結合
mst_pref <- mst_pref %>% 
  left_join(mst_kouseikyoku, by='厚生局') %>%
  print()

# 列名・列整理
mst_pref <- mst_pref %>% 
  select(厚生局コード,都道府県コード,都道府県名) %>% 
  arrange(厚生局コード,都道府県コード) %>% 
  print()

# DBにmst_prefを書き込み
DBI::dbWriteTable(con, 'mst_pref', mst_pref, overwrite=T) %>% print()

################################################################################

# 市町村コードのマスタを作成→すべて欠損だったので不要
# mst_shichoson <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
#   distinct(市町村コード,市町村名) %>% 
#   collect() %>% 
#   print()

################################################################################

# 区分を確認→医科だけにしてあるので不要列
# tbl(all_db_con, 'sisetukijun_all_update_date') %>%
#   count(区分) %>% 
#   print()

################################################################################

# 種別のマスタを作成
mst_syubetu <- tbl(all_db_con, 'sisetukijun_all_update_date') %>%
  distinct(種別コード,種別) %>%
  collect() %>% 
  arrange(種別コード) %>% 
  print()

# dbに書き込み
DBI::dbWriteTable(con, 'mst_syubetu', mst_syubetu, overwrite=T) %>% print()

################################################################################


################################################################################


# 届出マスタを作成
mst_todokede <- tbl(all_db_con, 'sisetukijun_all_update_date') %>%
  distinct(受理届出名称) %>%
  collect() %>% 
  arrange(受理届出名称) %>%
  rowid_to_column('受理届出コード') %>%
  print()

# 届出コードなしがありえるので対処
tmp <- tibble(受理届出コード = 0, 受理届出名称 = 'なし')
mst_todokede <- bind_rows(tmp, mst_todokede) %>% print()

# dbに書き込み
DBI::dbWriteTable(con, 'mst_todokede', mst_todokede, overwrite=T) %>% print()

################################################################################

# # mst_todokedeに点数本のコードを結合できるか検証
# mst_tensu <- readxl::read_excel('input/医科診療行為マスター.xlsx',col_types = 'text')
# mst_tensu %>% glimpse()
# mst_tensu <- mst_tensu %>% 
#   filter(年度=='2024')
# 
# tmp <- mst_todokede %>% 
#   left_join(mst_tensu,by=c('受理届出名称'='基本漢字名称')) %>% 
#   left_join(mst_tensu,by=c('受理届出名称'='省略漢字名称')) %>% 
#   glimpse()
# 
# tmp %>% 
#   writexl::write_xlsx('tmp/施設基準_点数マスタ結合チェック.xlsx')
# 
# # 結論→全然ダメ。特定集中治療すらマッチしていない。あきらめる

################################################################################

tbl(all_db_con, 'sisetukijun_all_update_date') %>% glimpse()

################################################################################

# 医療機関名称を改良した施設名列を作り、施設名で医療機関コードを特定できるようにする

# 最新update_dateの医療機関コードマスタを取得
mst_sisetu_name <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
  distinct(update_date,医療機関コード,医療機関名称,都道府県コード,`医療機関所在地（住所）`) %>% 
  group_by(医療機関コード) %>% 
  filter(update_date == max(update_date,na.rm=T)) %>%
  collect() %>% 
  glimpse() 

# 都道府県名を結合
mst_sisetu_name <- mst_sisetu_name %>% 
  left_join(mst_pref,by='都道府県コード') %>%
  print()

# どの条件でユニークになるかの判定用の演算
mst_sisetu_name <- mst_sisetu_name %>% 
  group_by(医療機関名称) %>%
  mutate(dpl_医療機関名称 = n()) %>% 
  ungroup() %>% 
  group_by(医療機関名称,都道府県名) %>%
  mutate(dpl_医療機関名称_都道府県名 = n()) %>%
  ungroup() %>%
  group_by(医療機関名称,都道府県名,`医療機関所在地（住所）`) %>%
  mutate(dpl_医療機関名称_都道府県名_住所 = n()) %>%
  ungroup()

mst_sisetu_name %>% count(dpl_医療機関名称) 
mst_sisetu_name %>% count(dpl_医療機関名称_都道府県名)
mst_sisetu_name %>% count(dpl_医療機関名称_都道府県名_住所)

# 施設名列を作成
mst_sisetu_name <- mst_sisetu_name %>% 
  mutate(施設名 = case_when(
    dpl_医療機関名称_都道府県名_住所 > 1 ~ str_glue('{医療機関名称}（{都道府県名}{`医療機関所在地（住所）`}_医療機関コード_{医療機関コード}）'),
    dpl_医療機関名称_都道府県名 > 1 ~ str_glue('{医療機関名称}（{都道府県名}{`医療機関所在地（住所）`}）'),
    dpl_医療機関名称 > 1 ~ str_glue('{医療機関名称}（{都道府県名}）'),
    TRUE ~ 医療機関名称
  )) %>%
  print()

# 確認用
# mst_sisetu_name %>%
#   filter(dpl_医療機関名称_都道府県名_住所 > 1) %>%
#   arrange(施設名) %>%
#   View()
# 
# mst_sisetu_name %>% 
#   filter(dpl_医療機関名称_都道府県名_住所 == 1) %>% 
#   filter(dpl_医療機関名称_都道府県名 > 1) %>% 
#   View()
# 
# mst_sisetu_name %>% 
#   filter(dpl_医療機関名称_都道府県名_住所 == 1) %>% 
#   filter(dpl_医療機関名称_都道府県名 == 1) %>% 
#   filter(dpl_医療機関名称 > 1) %>% 
#   View()

# 施設名でユニークになることを確認
mst_sisetu_name %>% 
  group_by(施設名) %>%
  filter(n()>1)

# error防止の為,distinctを入れておく
mst_sisetu_name <- mst_sisetu_name %>% 
  distinct(医療機関コード,施設名)

# dbに書き込み
dbWriteTable(con, 'mst_sisetu_name', mst_sisetu_name, overwrite = T)

################################################################################

# update_dateの一覧を取得
update_dates <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
  distinct(update_date) %>% 
  collect() %>% 
  arrange(update_date) %>%
  print()

################################################################################

# update_dateをループ処理
for (select_date in update_dates$update_date){
  
  print(select_date)
  
  # 病院一覧を取得
  sisetu  <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
    filter(update_date==select_date) %>%
    select(
      update_date
      ,医療機関コード
      ,併設医療機関コード
      ,厚生局
      ,都道府県コード
      # ,都道府県名
      # ,区分
      # ,医療機関番号
      # ,併設医療機関番号
      # ,医療機関記号番号
      ,医療機関名称
      ,郵便番号=`医療機関所在地（郵便番号）`
      ,住所 = `医療機関所在地（住所）`
      ,電話番号
      ,FAX番号
      ,病床数
      ,種別コード
    ) %>% 
    replace_na(list(
      電話番号=''
      ,FAX番号=''
      ,病床数=''
    )) %>% 
    distinct() %>% 
    collect() %>% 
    glimpse()
  
  # 厚生局コードに変換
  sisetu <- sisetu %>% 
    left_join(select(mst_kouseikyoku,厚生局,厚生局コード), by='厚生局') %>% 
    select(-厚生局) %>%
    print()
  
  # 施設名を結合
  sisetu <- sisetu %>% 
    inner_join(select(mst_sisetu_name,医療機関コード,施設名), by='医療機関コード') %>% 
    print()
  
  # 医療機関名、都道府県コード、住所でユニークにならない場合、医療機関コードが大きいものを優先して残す
  sisetu <- sisetu %>% 
    arrange(医療機関名称,都道府県コード,住所,desc(医療機関コード)) %>% 
    distinct(医療機関名称,都道府県コード,住所,.keep_all = T) %>% 
    print()
  
  # sisetu_mainテーブルとsisetu_subテーブルに分ける
  sisetu_main <- sisetu %>% 
    select(
      update_date
      ,医療機関コード
      ,施設名
      ,厚生局コード
      ,都道府県コード
      )
  
  sisetu_sub <- sisetu %>% 
    select(
      update_date
      ,医療機関コード
      ,併設医療機関コード
      ,医療機関名称
      ,郵便番号
      ,住所
      ,電話番号
      ,FAX番号
      ,病床数
      ,種別コード
    )
  
  # テーブルに書き込む
  DBI::dbWriteTable(con, 'sisetu_main', sisetu_main, append = TRUE) %>% print()
  DBI::dbWriteTable(con, 'sisetu_sub', sisetu_sub, append = TRUE) %>% print()
  
}

################################################################################

# update_dateをループ処理
for (select_date in update_dates$update_date){
  
  print(select_date)
  
  # 施設基準一覧を取得
  todokede  <- tbl(all_db_con, 'sisetukijun_all_update_date') %>% 
    filter(update_date==select_date) %>%
    select(
      update_date
      ,医療機関コード
      ,受理届出名称
      # ,受理記号
      # ,受理番号
      ,西暦算定開始年月日
    ) %>% 
    collect() %>%
    print()
  
  todokede <- todokede %>% 
    # 届け出が全くない医療機関について穴埋め
    replace_na(list(
      受理届出名称='なし'
      ,西暦算定開始年月日=''
    )) %>% 
    # 受理届出コードを結合して、受理届出名称を削除
    left_join(mst_todokede, by='受理届出名称') %>%
    select(-受理届出名称) %>%
    print()
  
  # 同じ医療機関・同じ届出で複数行になっていることがあるので重複削除
  todokede <- todokede %>% 
    # 算定開始年月日は古い日付を残す
    arrange(update_date,医療機関コード,受理届出コード,西暦算定開始年月日) %>% 
    distinct(update_date,医療機関コード,受理届出コード,.keep_all=T) %>% 
    print()
  
  # sisetukijun_allテーブルに書き込む
  DBI::dbWriteTable(con, 'todokede', todokede, append = TRUE) %>% print()
}

################################################################################

# 厚生局ごとにいつのデータが入っているかのデータを作る
mst_kouseikyoku_update_date <- tbl(con,'sisetu_main') %>% 
  distinct(厚生局コード,update_date) %>% 
  collect() %>% 
  arrange(厚生局コード,update_date) %>%
  print()

# dbに書き込み
DBI::dbWriteTable(con, 'mst_kouseikyoku_update_date', mst_kouseikyoku_update_date, overwrite=T) %>% print()

################################################################################

# 厚生局ごとの最新update_dateのsisetu_mainを作成
latest_sisetu_main <- tbl(con, 'mst_kouseikyoku_update_date') %>% 
  arrange(厚生局コード,desc(update_date)) %>% 
  distinct(厚生局コード,.keep_all=T) %>%
  inner_join(tbl(con,'sisetu_main'),by=c('update_date','厚生局コード')) %>% 
  collect() %>%
  print()

# dbに書き込み
DBI::dbWriteTable(con, 'latest_sisetu_main', latest_sisetu_main, overwrite=T) %>% print()

################################################################################

# 厚生局ごとの最新update_dateのsisetu_subを作成
latest_sisetu_sub <- tbl(con,'latest_sisetu_main') %>% 
  select(update_date,医療機関コード) %>% 
  inner_join(tbl(con,'sisetu_sub'),by=c('update_date','医療機関コード')) %>% 
  collect() %>%
  print()

# dbに書き込み
DBI::dbWriteTable(con, 'latest_sisetu_sub', latest_sisetu_sub, overwrite=T) %>% print()

################################################################################

# 厚生局ごとの最新update_dateのtodokedeを作成
latest_todokede <- tbl(con,'latest_sisetu_main') %>% 
  select(update_date,医療機関コード) %>% 
  inner_join(tbl(con,'todokede'),by=c('update_date','医療機関コード')) %>% 
  collect() %>% 
  print()

# dbに書き込み
DBI::dbWriteTable(con, 'latest_todokede', latest_todokede, overwrite=T) %>% print()

################################################################################

agg_get_date <- tbl(all_db_con,'sisetukijun_all_get_date') %>% 
  distinct(get_date,update_date,厚生局) %>% 
  collect() %>% 
  left_join(mst_kouseikyoku,by=c('厚生局')) %>% 
  print()

# 横長のexcel
agg_wide_get_date <- agg_get_date %>% 
  arrange(厚生局コード,get_date,update_date) %>% 
  pivot_wider(
    id_cols=get_date
    ,names_from=厚生局
    ,values_from=update_date
  ) %>% 
  print()

################################################################################

# チェック用に更新通知を作成
# 厚生局update_dateごとにmin(get_date)の列にフラグを立てる
update_notice <- agg_get_date %>% 
  group_by(厚生局,update_date) %>% 
  mutate(update = as.numeric(get_date == min(get_date,na.rm=T))) %>% 
  ungroup() %>% 
  group_by(get_date) %>% 
  mutate(update_count = sum(update)) %>% 
  ungroup()

# 更新メッセージを作成
update_notice <- update_notice %>% 
  mutate(message = case_when(
    update_count == 0 ~ '最新版への更新はありません'
    ,update_count > 0 & update == 1 ~ str_glue('{厚生局}のデータを{update_date}版に更新しました')
    ,update_count > 0 & update == 0 ~ '削除対象列'
  )) %>% 
  filter(message!='削除対象列') %>% 
  arrange(get_date,厚生局コード) %>% 
  distinct(get_date,message) %>% 
  print()

system("sudo chmod 666 更新状況.xlsx")

# 厚生局別_update_date一覧を出力
list(
  '更新通知'=update_notice
  ,'厚生局別get_date_update_date一覧'=agg_wide_get_date
  ) %>% 
  writexl::write_xlsx('更新状況.xlsx')


# update_noticeをdbに書き込み
DBI::dbWriteTable(con, 'update_notice', update_notice, overwrite=T) %>% print()

################################################################################

# 切断
DBI::dbDisconnect(all_db_con)
DBI::dbDisconnect(con)

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
