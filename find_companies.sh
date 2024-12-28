#!/bin/bash
#set -o noglob
declare -i count=0
#set file_list = `ls xa*`
#echo file_list: ${file_list}
#AWK_FILE=tx_only.awk 
range_list=i`awk -F';' '{print $1 "," $3}' na_15_chemicals.txt`
#echo "cd ./Pieces"
#cd ./Pieces
cwd=`pwd`
echo cwd: ${cwd}
TEMP_DIR="/tmp/find_companies/"
#mkdir ${TEMP_DIR}
#PIECES_DIR="/home/mtwheels66/DigitalElement/DE/TX/Pieces"
PIECES_DIR="./Pieces"
file_name=na_15_chemicals.txt
#file_name=na_15_sample.txt
echo "reading file: ${file_name}"
cat ${file_name} | while IFS=$";" read C1 C2 C3; do
    echo C1: ${C1}
    OUTPUT_FILE=${TEMP_DIR}/${count}.txt
    echo "grep '^${C1}' ${PIECES_DIR}/*"
    grep "^${C1}" ${PIECES_DIR}/* > ${OUTPUT_FILE}
    result=$?
    if [ ${result} -eq 0 ]; then
        echo "FOUND" 
        echo "${C3}" >> ${OUTPUT_FILE}
    else
        # File is empty, can remove it
        rm ${OUTPUT_FILE}
    fi
    ((count++))
done
