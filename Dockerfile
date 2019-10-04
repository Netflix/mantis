FROM java:8
MAINTAINER Mantis Developers <mantisdev@netflix.com>

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
    echo "deb http://repos.mesosphere.io/ubuntu trusty main" | tee /etc/apt/sources.list.d/mesosphere.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y --force-yes mesos=1.0.1-2.0.93.ubuntu1404 && \
    apt-get clean

COPY ./server/build/install/mantis-control-plane-server/bin/* /apps/mantis/mantis-control-plane-server/bin/
COPY ./server/build/install/mantis-control-plane-server/lib/* /apps/mantis/mantis-control-plane-server/lib/

COPY ./server/src/main/resources/master-docker.properties /apps/mantis/mantis-control-plane-server/conf/

RUN mkdir -p /apps/mantis/mantis-control-plane-server/src/main/webapp
RUN mkdir -p /apps/mantis/mantis-control-plane-server/logs
RUN mkdir -p /tmp/MantisSpool/namedJobs
RUN mkdir -p /tmp/MantisArchive

WORKDIR /apps/mantis/mantis-control-plane-server

ENTRYPOINT [ "bin/mantis-control-plane-server", "-p", "conf/master-docker.properties" ]
