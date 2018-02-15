INPUT_PATH="input/all/"

SPARK_BIN_PATH=/Users/shabbirhussain/Apps/spark-2.2.1-bin-hadoop2.7/bin/
SCALA_BIN_PATH=/usr/local/Cellar/scala@2.11/2.11.11/bin/

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME=target/artifacts/task.jar
LIB_PATH=target/dependency
RUNTIME_JARS=commons-csv-1.5.jar,json-20180130.jar,mysql-connector-java-8.0.9-rc.jar


COMMA=,
FULL_RUNTIME_JARS=${LIB_PATH}/$(subst ${COMMA},${COMMA}${LIB_PATH}/,${RUNTIME_JARS})

all: setup build run

build:
	mkdir -p "target/artifacts"
	mkdir -p "target/classes/main/resources/"
	${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
		-d target/classes \
		src/main/scala/org/reactorlabs/jshealth/**/*.scala \
		src/main/scala/org/reactorlabs/jshealth/*.scala
	cp -r src/main/resources/* target/classes
	cp src/main/shell/GHTorrent.sh target/classes
	jar cvfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C target/classes/ . &>/dev/null

run: build
	${SPARK_BIN_PATH}spark-submit \
	 	--master local --driver-memory 5g \
	 	--jars "${FULL_RUNTIME_JARS}" \
    	--class org.reactorlabs.jshealth.Main "${JAR_NAME}" "${INPUT_PATH}"

setup: clean
	mvn install dependency:copy-dependencies

clean:
	-rm -rf target/*