INPUT_PATH="input/all/"

SPARK_BIN_PATH=/Users/shabbirhussain/Apps/spark-2.2.1-bin-hadoop2.7/bin/
SCALA_BIN_PATH=/usr/local/Cellar/scala@2.11/2.11.11/bin/

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME="target/artifacts/task.jar"
LIB_PATH="target/dependency"

all: run

build:
	mkdir -p "target/artifacts"
	mkdir -p "target/classes/main/resources/"
	${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
		-d target/classes \
		src/main/scala/org/reactorlabs/git/downloader/**/*.scala \
		src/main/scala/org/reactorlabs/git/downloader/*.scala
	cp -r src/main/resources/* target/classes/
	jar cvfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C target/classes/ .
	# &>/dev/null

run: build
	${SPARK_BIN_PATH}spark-submit \
	 	--master local --driver-memory 6g \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    	--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    	--class org.reactorlabs.git.downloader.Main ${JAR_NAME} ${INPUT_PATH}

setup:
	mvn install dependency:copy-dependencies

clean:
	-rm -rf target/*
