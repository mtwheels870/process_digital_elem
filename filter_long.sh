#!/bin/bash
declare -i count=0
#set file_list = `ls xa*`
#echo file_list: ${file_list}
DIRECTORY=./Pieces
AWK_FILE=../ny_long.awk 
echo cd ${DIRECTORY}
cd ${DIRECTORY}
for filename  in `ls NY*txt`; do
    case "$filename" in
        *\.*)
            before_dot=${filename%%\.*}
            after_dot=${filename#*\.}
            echo "before/after=${before_dot}, ${after_dot}"
            new_file_name=${before_dot}_long.${after_dot}
            echo "Running Awk new_file_name = ${new_file_name}"
            awk -f ${AWK_FILE} ${filename} > ${new_file_name}
            ;;
        *)
            echo "Other filename: ${filename}"
            ;;
    esac
#    padded=$(seq -f "%02g" $count $count)
#    echo "awk -f ${AWK_FILE} ${filename} > TX${padded}.txt"
#    awk -f ${AWK_FILE} ${filename} > TX${padded}.txt
    ((count++))
done
