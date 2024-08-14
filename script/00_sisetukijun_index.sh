#!/bin/bash

cd /home/rstudio/srv/project/sisetukijun/

pwd

log=log/$(date '+%Y%m%d-%H%M%S').log

exec &> >(awk '{print strftime("[%Y/%m/%d %H:%M:%S]"),$0} {fflush()}' | tee -a $log)

Rscript -e "source('script/00_sisetukijun_index.R',echo=T)"
