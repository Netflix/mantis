FROM mesosphere/mesos-slave-dind:0.2.4_mesos-1.0.1_docker-1.12.1_ubuntu-14.04.5

MAINTAINER Mantis Developers <mantisd-oss-dev@netflix.com>

# Setup mantis-agent environment
RUN mkdir -p /mnt/local/mantisWorkerInstall/bin/
RUN mkdir -p /mnt/local/mantisWorkerInstall/libs/

# NOTE: Assumes you're building in the mantis-server-worker directory
COPY ./src/main/resources/startup_docker.sh /mnt/local/mantisWorkerInstall/bin/
COPY ./build/libs/mantis-server-worker*.jar /mnt/local/mantisWorkerInstall/libs/

# Set up Java 8
#ENV DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
#ENV CODENAME=$(lsb_release -cs)

RUN sudo apt-get update
RUN sudo apt-get install -y --no-install-recommends software-properties-common
RUN sudo apt-get install -y python-software-properties debconf-utils
RUN sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0xB1998361219BD9C9
RUN sudo apt-add-repository "deb http://repos.azulsystems.com/ubuntu stable main"
RUN sudo apt-get update
RUN sudo apt-get install -y zulu-8


# Make it look like we're running in VPC to test networking
ENV EC2_VPC_ID="FakeVpc"

# Present a ENI ResourceSet to the mesos. We expect the executor to handle this in test mode.
CMD ["mesos-slave", "--master=zk://127.0.0.1:2181/mesos", "--work_dir=/apps/mesos-agent", "--log_dir=/var/log/mesos/", "--logging_level=INFO", "--hostname=localmantisagent", "--attributes=region:laptop;asg:mantisagent-laptop-v001;stack:laptop;zone:laptopd;itype:macbook.pro;cluster:mantisagent-laptop;id:l-deadbeef;res:ResourceSet-ENIs-7-29"]
