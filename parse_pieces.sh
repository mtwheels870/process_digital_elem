#!/bin/bash
declare -i count=0
#set file_list = `ls xa*`
#echo file_list: ${file_list}
# AWK_FILE=tx_only.awk 
SPLITS_DIR=./Splits
echo cd ${SPLITS_DIR}
cd ${SPLITS_DIR}
#AWK_FILE=../ny_only.awk 
AWK_FILE=../fl_only.awk 
for filename  in `ls x*`; do
    padded=$(seq -f "%02g" $count $count)
    echo "awk -D -f ${AWK_FILE} ${filename} > FL${padded}.txt"
    awk -f ${AWK_FILE} ${filename} > FL${padded}.txt
    ((count++))
done
