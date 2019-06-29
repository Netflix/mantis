#!/usr/bin/env bash

# update /etc/hosts
update_etc_hosts () {
  if grep -Fxq "192.168.34.10 mantis-node1" /etc/hosts
  then
    echo "/etc/hosts already updated"
  else
    sudo sed -i 's/mantis-node1 //g' /etc/hosts
    sudo echo "192.168.34.10 mantis-node1" >> /etc/hosts
  fi
}

install_hotspot_jvm () {
  # install java
  if [ ! -f /home/vagrant/jdk-8u60-linux-x64.rpm ]; then
    echo "Downloading jdk-8u60-linux-x64.rpm"
    sudo wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jdk-8u60-linux-x64.rpm"
  fi

  sudo rpm -ivh jdk-8u60-linux-x64.rpm

}

setup_zookeeper () {
  # install and start zookeeper
  sudo rpm -Uvh http://archive.cloudera.com/cdh4/one-click-install/redhat/6/x86_64/cloudera-cdh-4-0.x86_64.rpm
  sudo yum -y install zookeeper zookeeper-server
  sudo -u zookeeper zookeeper-server-initialize --myid=1
  sudo service zookeeper-server start
}

setup_mesos () {
  # install mesos
  sudo rpm -Uvh http://repos.mesosphere.com/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm
  sudo yum -y install mesos-0.24.1

  # setup mesos slave with correct options
  sudo echo "reconnect" > /etc/mesos-slave/recover
  sudo echo "false" > /etc/mesos-slave/strict
  sudo echo "network:1024" > /etc/mesos-slave/resources

  # set zookeeper path for mesos
  sudo echo "zk://localhost:2181/mantis/mesos/vagrant" > /etc/mesos/zk
  sudo echo "1" /etc/mesos-master/quorum
  sudo echo "/var/lib/mesos" /etc/mesos-master/work_dir

  sudo service mesos-master start
  sudo service mesos-slave start
  sudo netstat -nlp | grep mesos

  export MASTER=$(mesos-resolve `cat /etc/mesos/zk` 2>/dev/null)
  mesos help
}

# setup worker env
setup_mantis_worker () {
  if [ ! -f /shared/mantisWorkerInstall/bin/startup.sh ]; then

    sudo mkdir -p /shared/mantisWorkerInstall/bin /shared/mantisWorkerInstall/libs
    cd /shared/mantis/mantis-server/mantis-server-worker
    /shared/mantis/gradlew clean fatJar

    sudo ln -s /shared/mantis/mantis-server/mantis-server-worker/src/main/resources/startup.sh /shared/mantisWorkerInstall/bin/startup.sh

    cd /shared/mantisWorkerInstall/libs
    sudo ln -s /shared/mantis/mantis-server/mantis-server-worker/build/libs/mantis-server-worker-*.jar

  fi
}

start_mantis_master () {
  sudo mkdir -p /logs/mantisjobs
  sudo chmod 777 /logs/mantisjobs/
  echo "Starting Mantis master...."
  cd /shared/mantis/mantis-server/mantis-server-master
  nohup /shared/mantis/gradlew runmaster -Dpropfile=/shared/mantis/mantis-localdev/conf/mantis-server-vagrant.properties > /home/vagrant/mantis_master.log 2>&1 &
}

wait_for_mantis_master () {
  response=$(curl --write-out %{http_code} --silent --output /dev/null http://192.168.34.10:7101/help)
  while [ $response != "200" ]; do
    sleep 10
    echo "mantis master API HTTP response code", $response
    response=$(curl --write-out %{http_code} --silent --output /dev/null http://192.168.34.10:7101/help)
  done
}

build_and_submit_job_zip () {
  if [ ! -f /shared/mantis/mantis-localdev/conf/sine-function-example.zip ]; then
    echo "Building example mantis job zip artifact"
    cd /shared/mantis/mantis-sdk/examples/sine-function
    /shared/mantis/gradlew clean build mantisZipArtifact

    cd /shared/mantis/mantis-localdev/conf
    sudo ln -s /shared/mantis/mantis-sdk/examples/sine-function/build/distributions/sine-function-1.0.zip sine-function-example.zip
  fi

  if [ -f /shared/mantis/mantis-localdev/conf/sine-function-example.zip ]; then
    # submit a named job to mantis master
    echo "Creating named job using mantis/mantis-localdev/conf/namedJob-template"
    curl -X POST http://192.168.34.10:7101/api/namedjob/create --silent --data @/shared/mantis/mantis-localdev/conf/namedJob-template

    echo "Submitting named job using mantis/mantis-localdev/conf/submitJob-template"
    curl -X POST http://192.168.34.10:7101/api/submit --silent --data @/shared/mantis/mantis-localdev/conf/submitJob-template
  else
    echo "job zip file does not exist @ /shared/mantis/mantis-localdev/conf/sine-function-example.zip, update the named_job template or manually create and submit a job to mantis-master at http://192.168.34.10:7101/api"
  fi
}

build_and_submit_job_jar () {
  if [ ! -f /shared/mantis/mantis-localdev/conf/sine-function-example.jar ]; then
    echo "Building example mantis job artifact"
    cd /shared/mantis/mantis-sdk/examples/sine-function
    /shared/mantis/gradlew clean build mantisArtifact

    cd /shared/mantis/mantis-localdev/conf
    sudo ln -s /shared/mantis/mantis-sdk/examples/sine-function/build/libs/sine-function-1.0.jar sine-function-example.jar
  fi

  if [ -f /shared/mantis/mantis-localdev/conf/sine-function-example.jar ]; then
    # submit a named job to mantis master
    echo "Creating named job using mantis/mantis-localdev/conf/namedJob-template"
    curl -X POST http://192.168.34.10:7101/api/namedjob/create --silent --data @/shared/mantis/mantis-localdev/conf/namedJob-template

    echo "Submitting named job using mantis/mantis-localdev/conf/submitJob-template"
    curl -X POST http://192.168.34.10:7101/api/submit --silent --data @/shared/mantis/mantis-localdev/conf/submitJob-template
  else
    echo "job jar file does not exist @ /shared/mantis/mantis-localdev/conf/sine-function-example.jar, update the named_job template or manually create and submit a job to mantis-master at http://192.168.34.10:7101/api"
  fi
}

update_etc_hosts
install_hotspot_jvm
setup_zookeeper
setup_mesos
setup_mantis_worker
start_mantis_master
wait_for_mantis_master
build_and_submit_job_zip

echo "The mantis-cluster setup is complete. You can now access the mesos UI at http://192.168.34.10:5050"
