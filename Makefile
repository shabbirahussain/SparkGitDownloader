INPUT_COMMAND=git

RUNTIME_JARS=commons-csv-1.5.jar,json-20180130.jar,mysql-connector-java-8.0.9-rc.jar,org.eclipse.jgit-5.0.1.201806211838-r.jar,jsch-0.1.54.jar

PRL_USER=hshabbir
PRL_MACHINE=${PRL_USER}@prl1c
PRL_DB_NAME=hshabbir_reactorlabs
NUM_WORKERS=*

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

PROJECT_NAME=SparkGitDownloader
DEPLOYABLE_TAR=target/${PROJECT_NAME}/
DEPLOYABLE_PAYLOAD_DIR=payload/

PWD=`pwd`/
COMMA=,
FULL_RUNTIME_JARS=${RUNTIME_LIB_PATH}$(subst ${COMMA},${COMMA}${RUNTIME_LIB_PATH},${RUNTIME_JARS})

all: setup build_all run

build_run: build run

clean_build: clean install_deps build

build: copy_resources
	scalac -feature -cp "./${LIB_PATH}*" \
		-d ${CLASSES_PATH} \
		src/main/scala/org/reactorlabs/jshealth/**/*.scala \
		src/main/scala/org/reactorlabs/jshealth/*.scala
	jar cfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C ${CLASSES_PATH} .
	rm -rf ${CLASSES_PATH}

run:
	spark-submit \
		--master local[${NUM_WORKERS}] \
		--files "${EXTRA_RESOURCES_PATH}conf/config-defaults.properties","${EXTRA_RESOURCES_PATH}conf/config.properties" \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration='file:${PWD}${EXTRA_RESOURCES_PATH}conf/log4j.properties'" \
	 	--jars ${FULL_RUNTIME_JARS} \
    	--class org.reactorlabs.jshealth.Main "${JAR_NAME}" "${INPUT_COMMAND}"

run_bg:
	spark-submit \
		--master local[${NUM_WORKERS}] \
		--files "${EXTRA_RESOURCES_PATH}conf/config-defaults.properties","${EXTRA_RESOURCES_PATH}conf/config.properties" \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration='file:${PWD}${EXTRA_RESOURCES_PATH}conf/log4j.properties'" \
		--jars ${FULL_RUNTIME_JARS} \
		--class org.reactorlabs.jshealth.Main "${JAR_NAME}" "${INPUT_COMMAND}" &

clean:
	-rm -rf target/*

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

DEPLOYABLE_DIR=target/deployable/
create_deployable: clean_build
	rm -rf ${DEPLOYABLE_DIR}
	mkdir -p ${DEPLOYABLE_DIR}${BUNDLE_DIR}
	cp Makefile ${DEPLOYABLE_DIR}
	cp -r ${BUNDLE_DIR} ${DEPLOYABLE_DIR}${BUNDLE_DIR}
	-cp -r ${DEPLOYABLE_PAYLOAD_DIR} ${DEPLOYABLE_DIR}${DEPLOYABLE_PAYLOAD_DIR}
	mkdir ${DEPLOYABLE_TAR}
	tar -C ${DEPLOYABLE_DIR} -cvf ${DEPLOYABLE_TAR}deployable.tar .
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

# =================================================================================
# PRL Specific scripts and configuration
# ---------------------------------------------------------------------------------
prl_deploy: create_deployable
	scp -r ${DEPLOYABLE_TAR} ${PRL_MACHINE}:/home/${PRL_USER}/

prl_ssh:
	ssh ${PRL_MACHINE}

prl_reset:
	rm -rf /mnt/ramdisk/hshabbir/repos/
	# mysql -p "${PRL_DB_NAME}" --execute='UPDATE REPOS_QUEUE SET CHECKOUT_ID=null WHERE COMPLETED=0;'

prl_tunnel:
	-open http://localhost:9040/jobs/
	-xdg-open http://localhost:9040/jobs/
	ssh -L 9040:localhost:4040 ${PRL_MACHINE} -t '\
		. /home/hshabbir/.bash_profile \
	    echo $$PATH;\
		cd ~/${PROJECT_NAME};\
		tar -xvf deployable.tar;\
		make prl_reset;\
		make run_bg;\
		bash -l;\
	'
prl_run: prl_deploy prl_tunnel

pmon:
	mysql -p "${PRL_DB_NAME}" --execute='SELECT * FROM PMON;'

# prl_kill: echo "ps auxf |grep '/home/hshabbir/Apps/jdk1.8.0_172/bin/java'|`awk '{ print "kill " $2 }'`"
# prl_fetch: scp -r ${PRL_MACHINE}:/tmp/hadoop-hshabbir/ght/ /tmp/hadoop-shabbirhussain/