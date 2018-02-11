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
[ -d "${DATA_DIR}" ] || mkdir "${DATA_DIR}"
cd "${DATA_DIR}"

# Download tar
echo -e "Starting download..."
curl -S "${GHT_URL}${TAR_NAME}" | \
    tar -C "${DATA_DIR}" -xv "${FILES[@]}" 2>&1 | \
    head -"${FILES_CNT}" > "${CTRL_FILE}"  2>&1 &

# Wait for the required files to be found in the tar
until [[ -s "${CTRL_FILE}" && $(wc -l "${CTRL_FILE}" | cut -d' ' -f 8) -ge "${FILES_CNT}" ]]; do
    sleep 10s
done

echo -e "\nFound required files from tar. Interrupting download!!"