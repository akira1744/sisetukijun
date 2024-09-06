# 全県の施設基準データを取得するスクリプト

## 厚生局からscriptを取得するコードは以下のlinkを参照した

https://github.com/higeme-gane/todokede

## 変更点

- 定期実行して、その歴を蓄積するように変更。

- 出力のフォルダ構成を変更
  - "output/実行日"以下にoriginalのExcelファイルと、全県の縦結合データのrdsファイル、厚生局別の更新日のcsvが出力されるように変更。

- 全県の縦結合データについて
  - get_date(出力日)とupdate_date(更新日)の列を追加
  - update_dateは、厚生局内でのmax(算定開始年月日)をし硫黄
  - 医療機関コードの列を追加
  - 西暦の日付を追加
  - 厚生局の列を追加

- scriptを定期実行するためのスクリプトを追加
  - windows上にdockerでRstudioServerを立てているので、定期実行用スクリプトが少し特殊

 
