FROM azul/zulu-openjdk:8u345

MAINTAINER Mantis Developers <mantis-oss-dev@netflix.com>

RUN apt-get -o Acquire::Check-Valid-Until=false -y update && \
    apt-get install -y gnupg && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
    apt-get install -y libevent-openssl-2.1.7 && \
    apt-get install -y libcurl3-gnutls libcurl3-nss && \
    echo "deb http://repos.mesosphere.com/ubuntu xenial main" | tee /etc/apt/sources.list.d/mesosphere.list && \
    apt-get -o Acquire::Check-Valid-Until=false -y update && \
    apt-get install -y mesos=1.3.2-2.0.1 && \
    apt-get clean

COPY ./mantis-control-plane-server/build/install/mantis-control-plane-server/bin/* /apps/mantis/mantis-control-plane-server/bin/
COPY ./mantis-control-plane-server/build/install/mantis-control-plane-server/lib/* /apps/mantis/mantis-control-plane-server/lib/

COPY ./mantis-control-plane-server/src/main/resources/master-docker.properties /apps/mantis/mantis-control-plane-server/conf/

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
