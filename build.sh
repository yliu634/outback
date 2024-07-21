#!/bin/bash
# 1 download wiki dataset

folder="./datasets/"
datasets=("wiki_ts_200M_uint64" "fb_200M_uint64" "osm_cellids_200M_uint64" "books_200M_uint64")
urls=(
  "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/SVN8PI"
  "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/EATHF7"
  "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/8FX9BV"
  "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/A6HDNT"
)

download="$1"
if [ "$download" == "1" ]; then
    file_count=$(ls -1 "$folder" | wc -l)
    if [ "$file_count" -eq 1 ]; then
        cd "$folder" || exit
        for ((i=0; i<${#datasets[@]}; i++)); do
            dataset="${datasets[i]}"
            url="${urls[i]}"
            wget -O - ${url} | zstd -d > ${dataset}
        done
        cd ..
    fi
fi

mkdir build
cd build
cmake .. && make -j

