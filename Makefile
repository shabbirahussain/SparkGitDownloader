INPUT_COMMAND=git

SPARK_BIN_PATH=
SCALA_BIN_PATH=
RUNTIME_JARS=commons-csv-1.5.jar,json-20180130.jar,mysql-connector-java-8.0.9-rc.jar,org.eclipse.jgit-4.8.0.201706111038-r.jar,jsch-0.1.54.jar,config-1.3.3.jar,akka-actor_2.11-2.5.11.jar,akka-stream-experimental_2.11-2.0.5.jar,reactive-streams-1.0.2.jar

AWS_MACHINE=hadoop@ec2-52-14-153-87.us-east-2.compute.amazonaws.com
AWS_PEM_PATH=~/ssh.pem

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME=target/artifacts/task.jar
LIB_PATH=target/dependency/
AWS_DIR=target/aws/
CLASSES_PATH=target/classes/
RESOURCES_PATH=${CLASSES_PATH}resources/

PWD=`pwd`/
COMMA=,
FULL_RUNTIME_JARS=${LIB_PATH}$(subst ${COMMA},${COMMA}${LIB_PATH},${RUNTIME_JARS})

all: setup build_all run

build_run: build run

build_all: clean install_deps build

build:
	mkdir -p "target/artifacts"
	mkdir -p "${RESOURCES_PATH}scripts"
	cp -r src/main/resources/* ${RESOURCES_PATH}
	cp -r src/main/shell ${RESOURCES_PATH}scripts/shell
	cp -r src/main/mysql ${RESOURCES_PATH}scripts/mysql
	${SCALA_BIN_PATH}scalac -feature -cp "./${LIB_PATH}*" \
		-d target/classes \
		src/main/scala/org/reactorlabs/jshealth/**/*.scala \
		src/main/scala/org/reactorlabs/jshealth/*.scala
	jar cfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C target/classes/ .

run:
	${SPARK_BIN_PATH}spark-submit \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration='file:${PWD}${RESOURCES_PATH}conf/log4j.properties'" \
		--driver-memory 15g --executor-memory 15G \
	 	--jars "${FULL_RUNTIME_JARS}" \
    	--class org.reactorlabs.jshealth.Main "${JAR_NAME}" ${INPUT_COMMAND}

install_deps:
	mvn install dependency:copy-dependencies

clean:
	-rm -rf target/*

setup:
	ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
	brew install git-lfs
	brew install maven
	brew install apache-spark
	brew install scala@2.11
	brew install mysql
	brew tap homebrew/services
	brew services start mysql

# =================================================================================
# AWS Specific scripts and configuration
# ---------------------------------------------------------------------------------

aws_ss:
	spark-shell --driver-memory 5G --executor-memory 5G \
	--jars=${FULL_RUNTIME_JARS} \
	--conf spark.scheduler.mode=FAIR \
	--conf spark.checkpoint.compress=true \
	--conf spark.executor.extraClassPath="/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*" \
	--conf spark.driver.extraClassPath="/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*"

aws_deploy: build_all
	rm -rf ${AWS_DIR}
	mkdir -p ${AWS_DIR}/${LIB_PATH}
	cp ${LIB_PATH}/{${RUNTIME_JARS}} ${AWS_DIR}/${LIB_PATH}
	cp ${JAR_NAME} ${AWS_DIR}/${JAR_NAME}
	cp Makefile ${AWS_DIR}
	scp -r -i ${AWS_PEM_PATH} ${AWS_DIR} ${AWS_MACHINE}:/mnt/project

aws_ssh:
	ssh -i ${AWS_PEM_PATH} ${AWS_MACHINE} << EOF
		cd /mnt/project
	EOF

aws: aws_deploy aws_ssh