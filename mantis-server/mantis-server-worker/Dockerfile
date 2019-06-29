FROM mesosphere/mesos-slave-dind:0.2.4_mesos-1.0.1_docker-1.12.1_ubuntu-14.04.5

MAINTAINER Mantis Developers <mantisdev@netflix.com>

# Setup mantis-agent environment
RUN mkdir -p /mnt/local/mantisWorkerInstall/bin/
RUN mkdir -p /mnt/local/mantisWorkerInstall/libs/

# NOTE: Assumes you're building in the mantis-server-worker directory
COPY ./src/main/resources/startup_docker.sh /mnt/local/mantisWorkerInstall/bin/
COPY ./build/libs/mantis-server-worker*.jar /mnt/local/mantisWorkerInstall/libs/

# setup Java 8
RUN apt-get update
RUN apt-get install -y --no-install-recommends software-properties-common
RUN apt-get install -y python-software-properties debconf-utils
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
RUN apt-get install -y oracle-java8-installer
RUN apt-get install -y oracle-java8-unlimited-jce-policy
RUN apt-get install -y oracle-java8-set-default
RUN update-alternatives --config java
RUN update-alternatives --config javac

# Make it look like we're running in VPC to test networking
ENV EC2_VPC_ID="FakeVpc"

# Present a ENI ResourceSet to the mesos. We expect the executor to handle this in test mode.
CMD ["mesos-slave", "--master=zk://127.0.0.1:2181/mesos", "--work_dir=/apps/mesos-agent", "--log_dir=/var/log/mesos/", "--logging_level=INFO", "--hostname=localmantisagent", "--attributes=region:laptop;asg:mantisagent-laptop-v001;stack:laptop;zone:laptopd;itype:macbook.pro;cluster:mantisagent-laptop;id:l-deadbeef;res:ResourceSet-ENIs-7-29"]