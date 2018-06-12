INPUT_COMMAND=git

SPARK_BIN_PATH=
SCALA_BIN_PATH=
RUNTIME_JARS=commons-csv-1.5.jar,json-20180130.jar,mysql-connector-java-8.0.9-rc.jar,org.eclipse.jgit-4.8.0.201706111038-r.jar,jsch-0.1.54.jar
#,reactive-streams-1.0.2.jar,jctools-core-2.0.2.jar,config-1.3.3.jar,akka-actor_2.11-2.5.11.jar,akka-stream-experimental_2.11-2.0.5.jar
#,monix-eval_2.11-3.0.0-8084549.jar,monix-execution_2.11-3.0.0-8084549.jar,monix-reactive_2.11-3.0.0-8084549.jar,cats-core_2.11-1.0.0-RC1.jar,cats-kernel_2.11-1.0.0-RC1.jar,cats-effect_2.11-0.5.jar,monix-tail_2.11-3.0.0-8084549.jar


AWS_MACHINE=hadoop@ec2-52-14-153-87.us-east-2.compute.amazonaws.com
AWS_PEM_PATH=~/ssh.pem

PRL_USER=hshabbir
PRL_MACHINE=${PRL_USER}@prl1c
PRL_DB_NAME=hshabbir_reactorlabs
NUM_WORKERS=40

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
BUNDLE_DIR=target/bundle/
JAR_NAME=${BUNDLE_DIR}task.jar
CLASSES_PATH=target/classes/

LIB_PATH=target/dependency/
RUNTIME_LIB_PATH=${BUNDLE_DIR}dependency/

EXTRA_RESOURCES_PATH=${BUNDLE_DIR}resources/
PACKAGED_RESOURCES_PATH=${CLASSES_PATH}resources/

DEPLOYABLE_DIR=target/deployable/
DEPLOYABLE_TAR=target/deployable.tar
DEPLOYABLE_PAYLOAD_DIR=payload/

PWD=`pwd`/
COMMA=,
FULL_RUNTIME_JARS=${RUNTIME_LIB_PATH}$(subst ${COMMA},${COMMA}${RUNTIME_LIB_PATH},${RUNTIME_JARS})

all: setup build_all run

build_run: build run

clean_build: clean install_deps build

build: copy_resources
	${SCALA_BIN_PATH}scalac -feature -cp "./${LIB_PATH}*" \
		-d ${CLASSES_PATH} \
		src/main/scala/org/reactorlabs/jshealth/**/*.scala \
		src/main/scala/org/reactorlabs/jshealth/*.scala
	jar cfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C ${CLASSES_PATH} .
	rm -rf ${CLASSES_PATH}

run:
	${SPARK_BIN_PATH}spark-submit \
		--master local[${NUM_WORKERS}] \
		--files "${EXTRA_RESOURCES_PATH}conf/config-defaults.properties" \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration='file:${PWD}${EXTRA_RESOURCES_PATH}conf/log4j.properties'" \
	 	--jars ${FULL_RUNTIME_JARS} \
    	--class org.reactorlabs.jshealth.Main "${JAR_NAME}" "${INPUT_COMMAND}" &

install_deps:
	mvn install dependency:copy-dependencies

copy_resources:
	mkdir -p "target/bundle/dependency"
	rm -rf "${CLASSES_PATH}"
	mkdir -p "${CLASSES_PATH}"
	mkdir -p "${PACKAGED_RESOURCES_PATH}scripts"
	cp ${LIB_PATH}{${RUNTIME_JARS}} "target/bundle/dependency/"
	cp -r src/main/resources/ ${EXTRA_RESOURCES_PATH}
	cp -r src/main/{shell,mysql} "${PACKAGED_RESOURCES_PATH}scripts/"

clean:
	-rm -rf target/*

create_deployable: clean_build
	rm -rf ${DEPLOYABLE_DIR}
	mkdir -p ${DEPLOYABLE_DIR}${BUNDLE_DIR}
	cp Makefile ${DEPLOYABLE_DIR}
	cp -r ${BUNDLE_DIR} ${DEPLOYABLE_DIR}${BUNDLE_DIR}
	-cp -r ${DEPLOYABLE_PAYLOAD_DIR} ${DEPLOYABLE_DIR}${DEPLOYABLE_PAYLOAD_DIR}
	tar -C ${DEPLOYABLE_DIR} -cvf target/deployable.tar .
	rm -rf ${DEPLOYABLE_DIR}

setup:
	ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
	brew install git-lfs
	brew install maven
	brew install apache-spark
	brew install scala@2.11
	brew install mysql
	brew tap homebrew/services
	brew services start mysql

ss:
	spark-shell --driver-memory 5G --executor-memory 5G \
	--master local[50] \
	--jars=${FULL_RUNTIME_JARS} \
	--conf spark.scheduler.mode=FAIR \
	--conf spark.checkpoint.compress=true

# =================================================================================
# AWS Specific scripts and configuration
# ---------------------------------------------------------------------------------
AWS_EXTRA_CLASSPATH=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*

aws_ss:
	spark-shell --driver-memory 5G --executor-memory 5G \
	--jars=${FULL_RUNTIME_JARS} \
	--conf spark.scheduler.mode=FAIR \
	--conf spark.checkpoint.compress=true \
	--conf spark.executor.extraClassPath="${AWS_EXTRA_CLASSPATH}" \
	--conf spark.driver.extraClassPath="${AWS_EXTRA_CLASSPATH}"

aws_deploy: create_deployable
	scp -i "${AWS_PEM_PATH}" "${DEPLOYABLE_TAR}" "${AWS_MACHINE}:/mnt/project"

aws_ssh:
	ssh -i ${AWS_PEM_PATH} ${AWS_MACHINE} << EOF
		cd /mnt/project
	EOF

aws: aws_deploy aws_ssh

# =================================================================================
# PRL Specific scripts and configuration
# ---------------------------------------------------------------------------------
prl_deploy: create_deployable
	scp ${DEPLOYABLE_TAR} ${PRL_MACHINE}:/home/${PRL_USER}/Project

prl_ssh:
	ssh ${PRL_MACHINE}

prl_fetch:
	scp -r ${PRL_MACHINE}:/tmp/hadoop-hshabbir/ght/ /tmp/hadoop-shabbirhussain/

prl_kill:
	echo "ps auxf |grep '/home/hshabbir/Apps/jdk1.8.0_172/bin/java'|`awk '{ print "kill " $2 }'`"

prl_reset:
	rm -rf /mnt/ramdisk/hshabbir/repos/
	tar -xf deployable.tar
	cp target/bundle/resources/conf/config-prl.properties target/bundle/resources/conf/config-defaults.properties
	mysql -p "${PRL_DB_NAME}" --execute='UPDATE REPOS_QUEUE SET CHECKOUT_ID=null WHERE COMPLETED=0;'

pmon:
	mysql -p "${PRL_DB_NAME}" --execute='SELECT * FROM PMON;'