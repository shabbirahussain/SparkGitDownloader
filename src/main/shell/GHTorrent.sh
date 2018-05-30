#!/usr/bin/env bash
# This scripts downloads tar from GHTorrent and extracts only the listed files from it. In case no files are
# specified it will extract all files.
#
# @author: shabbir.ahussain

GHT_URL="http://ghtorrent-downloads.ewi.tudelft.nl/mysql/"
CTRL_FILE="GHTorrent.ctrl"

show_usage(){
    echo -e "Usage: \n"
    echo -e "\t ${0} tar_name data_dir [file_to_txtract [...]]\n"
    echo -e "Example: \n"
    echo -e "\t ./GHTorrent.sh mysql-2018-02-01.tar.gz /Users/shabbirhussain/Data/project mysql-2018-02-01/projects.csv"
    echo -e "\t ./GHTorrent.sh mysql-2018-02-01.tar.gz /Users/shabbirhussain/Data/project mysql-2018-02-01/commit_comments.csv"
}

cleanup(){
    echo -e "\nCleaning up background jobs..."
    kill $(jobs -p) 2> /dev/null
    echo -e "Done."
}

if [[ "$#" -lt 2 ]]; then
    echo -e "Error: Invalid number of arguments" >&2
    show_usage
    exit 1
fi

TAR_NAME="$1"
DATA_DIR="$2"

CTRL_FILE="${DATA_DIR}/${CTRL_FILE}"
FILES=("$@")
FILES=( "${FILES[@]:2}" )

FILES_CNT=$(expr "$#" - 2)

set -e
trap 'cleanup' EXIT

# Create data directory
[ -d "${DATA_DIR}" ] || mkdir -p "${DATA_DIR}"
cd "${DATA_DIR}"

# Download tar
echo -e "Starting download..."
echo -e "curl -S ${GHT_URL}${TAR_NAME} | tar -C ${DATA_DIR} -xvz ${FILES[@]} 2>&1 | head -${FILES_CNT} > ${CTRL_FILE}  2>&1"

curl -S "${GHT_URL}${TAR_NAME}" | \
    tar -C "${DATA_DIR}" -xvz "${FILES[@]}" 2>&1 | \
    head -"${FILES_CNT}" > "${CTRL_FILE}"  2>&1 # &
#CURL_PID=$!
#echo -e "Background CURL_PID=${CURL_PID}"
#
## Wait for the required files to be found in the tar
#until [[ ! -n "$(ps -p ${CURL_PID} -o pid=)" ]]; do
#    sleep 10s
#done

echo -e "\nFound required file(s) from tar. Interrupting download!!"

#echo -e "\nZipping all file individually..."
#gzip -r "${DATA_DIR}"