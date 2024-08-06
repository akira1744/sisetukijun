#!/bin/bash

cd /home/rstudio/srv/project/20240806_施設基準取得/

pwd

log=log/$(date '+%Y%m%d-%H%M%S').log

exec &> >(awk '{print strftime("[%Y/%m/%d %H:%M:%S]"),$0} {fflush()}' | tee -a $log)

Rscript -e "source('script/01_get_sisetukijun.R',echo=T)"
