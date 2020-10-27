FROM docker-hub.netflix.net/mesosphere/mesos-slave:1.3.2

MAINTAINER Mantis Developers <mantisd-oss-dev@netflix.com>

RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

ENV MESOS_SYSTEMD_ENABLE_SUPPORT false

# Setup mantis-agent environment
RUN mkdir -p /mnt/local/mantisWorkerInstall/bin/
RUN mkdir -p /mnt/local/mantisWorkerInstall/libs/
RUN mkdir -p /mnt/local/mantisWorkerInstall/jobs/

# NOTE: Assumes you're building in the mantis-server-worker directory
COPY ./src/main/resources/startup_docker.sh /mnt/local/mantisWorkerInstall/bin/
COPY ./build/libs/mantis-server-worker*.jar /mnt/local/mantisWorkerInstall/libs/
COPY worker-docker.properties /mnt/local/mantisWorkerInstall/jobs/

# Set up Java 8
RUN apt-get update
RUN apt-get install -y --no-install-recommends software-properties-common
RUN apt-get install -y python3-software-properties debconf-utils
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0xB1998361219BD9C9
RUN apt-add-repository "deb http://repos.azulsystems.com/ubuntu stable main"
RUN apt-get install -y unzip
RUN apt-get install wget
RUN apt-get update
RUN apt-get install -y zulu-8


# Make it look like we're running in VPC to test networking
ENV EC2_VPC_ID="FakeVpc"

# Present a ENI ResourceSet to the mesos. We expect the executor to handle this in test mode.
CMD ["mesos-slave", "--master=zk://127.0.0.1:2181/mesos", "--work_dir=/apps/mesos-agent", "--log_dir=/var/log/mesos/", "--logging_level=INFO", "--hostname=localmantisagent", "--attributes=region:laptop;asg:mantisagent-laptop-v001;stack:laptop;zone:laptopd;itype:macbook.pro;cluster:mantisagent-laptop;id:l-deadbeef;res:ResourceSet-ENIs-7-29"]
