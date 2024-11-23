rm(list=ls())

pacman::p_load(
  here
  ,DBI
  ,writexl
  ,lubridate
  ,duckdb
  ,duckplyr
  ,arrow
  ,tidyverse
  ,tidylog
)

output_dir <- here('output') %>% print()

################################################################################

# すべてのparquetを格納するdb
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
  distinct(update_date,厚生局) %>% 
  filter(update_date>='2024-08-01') %>% 
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

# 届出マスタを作成
mst_todokede <- tbl(all_db_con, 'sisetukijun_all_update_date') %>%
  distinct(受理届出名称) %>%
  filter(!is.na(受理届出名称)) %>% 
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
  filter(update_date>='2024-08-01') %>% 
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
    collect() %>% 
    distinct() %>% 
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
  group_by(厚生局コード,update_date) %>% 
  summarise(tmp=1) %>% # collectの前にdistinctするとarrangeのwarningが発生するのでgroup_byとsummariceに変更
  collect() %>% 
  select(厚生局コード,update_date) %>% 
  arrange(厚生局コード,update_date) %>%
  print()

# dbに書き込み
DBI::dbWriteTable(con, 'mst_kouseikyoku_update_date', mst_kouseikyoku_update_date, overwrite=T) %>% print()

################################################################################

# 厚生局ごとの最新update_dateのsisetu_mainを作成
latest_sisetu_main <- tbl(con, 'mst_kouseikyoku_update_date') %>% 
  group_by(厚生局コード) %>% 
  summarise(update_date = max(update_date,na.rm=T)) %>% 
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
  filter(update_date>='2024-08-01') %>% 
  left_join(mst_kouseikyoku,by=c('厚生局')) %>% 
  print()

# 採用フラグを作成
agg_saiyou_get_date <- agg_get_date %>% 
  group_by(厚生局,update_date) %>% 
  mutate(採用 = as.numeric(get_date==max(get_date))) %>% 
  ungroup() %>% 
  group_by(get_date) %>% 
  summarise(採用 = max(採用,na.rm=T)) %>% 
  print()

# 横長のexcel
agg_wide_get_date <- agg_get_date %>% 
  arrange(厚生局コード) %>% 
  pivot_wider(
    id_cols=get_date
    ,names_from=厚生局
    ,values_from=update_date
  ) %>% 
  arrange(get_date) %>% 
  print()

# 採用フラグを結合
agg_wide_get_date <- agg_wide_get_date %>% 
  left_join(agg_saiyou_get_date,by='get_date') %>% 
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
  ungroup() %>% 
  print()

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

update_notice %>% 
  write_csv('output/更新通知.csv')

agg_wide_get_date %>% 
  write_csv('output/厚生局別get_date_update_date.csv')

# update_noticeをdbに書き込み
DBI::dbWriteTable(con, 'update_notice', update_notice, overwrite=T) %>% print()

DBI::dbWriteTable(con, 'update_dates', agg_wide_get_date, overwrite=T) %>% print()

################################################################################

# update_dateエラー検出

# script:00.2で作成した更新日を読み込み
org_update_dates <- read_csv('output/更新日.csv') %>% print()

org_update_date_long <- org_update_dates %>% 
  pivot_longer(-date,names_to='厚生局',values_to='org_update_date') %>% 
  mutate(org_update_date = str_replace(org_update_date,'現在','')) %>% 
  mutate(org_update_date = convert_jdate(org_update_date)) %>%
  rename(get_date = date) %>% 
  print()

agg_get_date <- agg_get_date %>% 
  mutate(get_date=ymd(get_date)) %>% 
  mutate(update_date = ymd(update_date)) %>% 
  select(-厚生局コード) %>% 
  print()

check_update_date <- org_update_date_long %>% 
  left_join(agg_get_date,by=c('get_date','厚生局')) %>% 
  filter(is.na(update_date)) %>% 
  print()

# 更新日エラーがあったら出力
if(nrow(check_update_date)>0){
  check_update_date %>% 
    write_csv('output/update_dateエラー.csv')
}

################################################################################

# 切断
DBI::dbDisconnect(all_db_con)
DBI::dbDisconnect(con)

################################################################################


