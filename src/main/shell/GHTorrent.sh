#!/usr/bin/env bash
# This scripts downloads tar from GHTorrent and extracts only the listed files from it.
# In case no files are specified it will extract all files.
# @author: shabbir.ahussain
#

#    DownloadGhTorrent : () => {
#        let args = process.argv.slice(3);
#        if (args.length < 2) {
#            console.log("Invalid number of arguments");
#            console.log("see README.md");
#            process.exit(-1);
#        }
#        let name = args[0]
#        let outputDir = args[1];
#        if (!outputDir.endsWith("/"))
#            outputDir += "/";
#        let discardData = false;
#        for (let i = 2; i < args.length; ++i) {
#            if (args[i] == "--discard-data") {
#                discardData = true;
#            } else {
#                console.log("unknown argument: " + args[i]);
#                console.log("see README.md");
#                process.exit(-1);
#            }
#        }
#        console.log("ghtorrent.name = " + name);
#        console.log("ghtorrent.outputDir = " + outputDir);
#        console.log("ghtorrent.keepData = " + discardData);
#
#        mkdirp.sync(outputDir + name);
#        console.log("downloading ghtorrent dump...")
#        let dumpFile = outputDir + name + "/ghtorrent.tar.gz";
#        child_process.execSync("curl -S http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-"+ name + ".tar.gz > " + dumpFile);
#        console.log("extracting projects table...");
#        child_process.execSync("tar --extract --to-stdout --file=" + dumpFile + " mysql-" + name + "/projects.csv > " + outputDir + name + "/projects.csv");
#        if (discardData) {
#            console.log("deleting the snapshot...");
#            child_process.execSync("rm -f " + dumpFile);
#        }
#        console.log("DONE.");
#    }

GHT_URL="http://ghtorrent-downloads.ewi.tudelft.nl/mysql/"
CTRL_FILE="GHTorrent.ctrl"

show_usage(){
    echo -e "Usage: \n"
    echo -e "\t ${0} tar_name data_dir [file_to_txtract [...]]\n"
    echo -e "Example: \n"
    echo -e "\t ./GHTorrent.sh mysql-2018-02-01.tar.gz /Users/shabbirhussain/Data/project mysql-2018-02-01/projects.csv"
    echo -e "\t ./GHTorrent.sh mysql-2018-02-01.tar.gz /Users/shabbirhussain/Data/project mysql-2018-02-01/commit_comments.csv"
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

echo "${FILES[@]}"
echo "${CTRL_FILE}"

set -e
trap 'kill $(jobs -p) 2> /dev/null' EXIT

# Create a pipe
PIPE=$(mktemp)
rm "${PIPE}"
mkfifo "${PIPE}"


# Create data directory
[ -d "${DATA_DIR}" ] || mkdir "${DATA_DIR}"
cd "${DATA_DIR}"

# Download tar
echo -e "Starting download..."
curl -S "${GHT_URL}${TAR_NAME}" > "${PIPE}" &
PID="$!"

# Un-tar required files from tar
tar -C "${DATA_DIR}" -xvf "${PIPE}" "${FILES[@]}" 2>&1 | head -"${FILES_CNT}" > "${CTRL_FILE}" 2>&1 &

until [[ -s "${CTRL_FILE}" && $(wc -l "${CTRL_FILE}") -ge "${FILES_CNT}" ]]; do
    #echo "sleeping"
    sleep 10s
done

kill "${PID}"