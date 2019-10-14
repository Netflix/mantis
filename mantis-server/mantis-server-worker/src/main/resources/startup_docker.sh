#!/bin/bash

set -x
#172.16.186.7:7101 mantis api host port in docker
echo "Executing script to download file at: ${JOB_URL}, storing in /tmp/mantis-jobs/${JOB_NAME}/lib"
ARTIFACT_NAME=$(echo ${JOB_URL:7})
JOB_URL=`echo \${JOB_URL} | sed 's|http://|&172.16.186.7:7101/api/v1/artifacts/|g'`
echo "Executing script to download file at: ${JOB_URL}, storing in /tmp/mantis-jobs/${JOB_NAME}/lib"

mkdir -p /tmp/mantis-jobs/${JOB_NAME}/lib
mkdir -p /logs/mantisjobs/${JOB_NAME}/${JOB_ID}/${WORKER_NUMBER}

EXTRA_OPTS="-Dcom.sun.management.jmxremote=true \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.host=localhost \
-Dcom.sun.management.jmxremote.port=$MANTIS_WORKER_DEBUG_PORT"

JAVA_OPTS=" $EXTRA_OPTS \
-Xmx${JVM_MEMORY_MB}m \
-XX:+PrintGCDetails \
-XX:+PrintGCTimeStamps \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=/logs/mantisjobs/${JOB_NAME}/${JOB_ID}/${WORKER_NUMBER} \
-Xloggc:/logs/mantisjobs/${JOB_NAME}/${JOB_ID}/${WORKER_NUMBER}/gc.log \
-XX:MaxDirectMemorySize=256m"

JVM_CLASSPATH="${WORKER_LIB_DIR}/*"
JOB_JARS_DIR="/tmp/mantis-jobs/${JOB_NAME}/lib"
JOB_PROVIDER_CLASS=""
wget -v $JOB_URL -P "/tmp/mantis-jobs/${JOB_NAME}/lib"
#java -cp $JVM_CLASSPATH io.mantisrx.server.worker.DownloadJob ${JOB_URL} ${JOB_NAME} /tmp/mantis-jobs/${JOB_NAME}/lib
#cp -r /mnt/local/mantisWorkerInstall/jobs/* "/tmp/mantis-jobs/${JOB_NAME}/lib"
# Link worker.properties
cp -s /mnt/local/mantisWorkerInstall/jobs/* "/tmp/mantis-jobs/${JOB_NAME}/lib"
# Link mantis-worker.jar
cp -s /mnt/local/mantisWorkerInstall/libs/* "/tmp/mantis-jobs/${JOB_NAME}/lib"
cd $JOB_JARS_DIR

zipexists=`ls -l $ARTIFACT_NAME 2>/dev/null | wc -l`

if [ $zipexists = 1 ]
then
    sudo apt install -y unzip
    mkdir zipExtract
    unzip $ARTIFACT_NAME -d zipExtract
    JOB_PROVIDER_CLASS=`cat zipExtract/*/config/io.mantisrx.runtime.MantisJobProvider`
    echo "job provider class $JOB_PROVIDER_CLASS"
    ZIP_LIB_DIR=`echo $JOB_JARS_DIR/zipExtract/*/lib`
    JVM_CLASSPATH="$ZIP_LIB_DIR/*:$JVM_CLASSPATH"
else
    JVM_CLASSPATH="$JVM_CLASSPATH:$JOB_JARS_DIR/*"
fi

echo "Executing Mantis Worker java $JAVA_OPTS -cp $JVM_CLASSPATH -DMASTER_DESCRIPTION="${MASTER_DESCRIPTION}" -DJOB_PROVIDER_CLASS="$JOB_PROVIDER_CLASS" io.mantisrx.server.worker.MantisWorker"
java $JOB_PARAM_MANTIS_WORKER_JVM_OPTS $JAVA_OPTS -cp $JVM_CLASSPATH -DMASTER_DESCRIPTION="${MASTER_DESCRIPTION}" -DJOB_PROVIDER_CLASS="$JOB_PROVIDER_CLASS" io.mantisrx.server.worker.MantisWorker -p /mnt/local/mantisWorkerInstall/jobs/worker-docker.properties


 
 
 




