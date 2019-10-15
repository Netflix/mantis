FROM java:8
MAINTAINER Mantis Developers <mantis-oss-dev@netflix.com>

RUN echo "deb [check-valid-until=no] http://cdn-fastly.deb.debian.org/debian jessie main" > /etc/apt/sources.list.d/jessie.list
RUN echo "deb [check-valid-until=no] http://archive.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list
RUN sed -i '/deb http:\/\/deb.debian.org\/debian jessie-updates main/d' /etc/apt/sources.list
RUN apt-get -o Acquire::Check-Valid-Until=false update

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
    echo "deb http://repos.mesosphere.com/ubuntu trusty main" | tee /etc/apt/sources.list.d/mesosphere.list && \
    apt-get -o Acquire::Check-Valid-Until=false -y update && \
    apt-get install -y mesos=1.0.1-2.0.93.ubuntu1404 && \
    apt-get clean

COPY ./server/build/install/mantis-control-plane-server/bin/* /apps/mantis/mantis-control-plane-server/bin/
COPY ./server/build/install/mantis-control-plane-server/lib/* /apps/mantis/mantis-control-plane-server/lib/

COPY ./server/src/main/resources/master-docker.properties /apps/mantis/mantis-control-plane-server/conf/

RUN mkdir -p /apps/mantis/mantis-control-plane-server/src/main/webapp
RUN mkdir -p /apps/mantis/mantis-control-plane-server/logs
RUN mkdir -p /tmp/MantisSpool/namedJobs
RUN mkdir -p /tmp/MantisArchive

COPY docker/SharedMrePublishEventSource /tmp/MantisSpool/namedJobs
COPY docker/SineFunction /tmp/MantisSpool/namedJobs
COPY docker/SyntheticSourceJob /tmp/MantisSpool/namedJobs
COPY docker/TwitterSample /tmp/MantisSpool/namedJobs
COPY docker/GroupBySample /tmp/MantisSpool/namedJobs
COPY docker/JobConnectorSample /tmp/MantisSpool/namedJobs
COPY docker/KafkaSourceJob /tmp/MantisSpool/namedJobs

WORKDIR /apps/mantis/mantis-control-plane-server

ENTRYPOINT [ "bin/mantis-control-plane-server", "-p", "conf/master-docker.properties" ]
