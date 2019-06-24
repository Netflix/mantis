FROM java:8
MAINTAINER Mantis Developers <mantisdev@netflix.com>

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
    echo "deb http://repos.mesosphere.io/ubuntu trusty main" | tee /etc/apt/sources.list.d/mesosphere.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y --force-yes mesos=1.0.1-2.0.93.ubuntu1404 && \
    apt-get clean

COPY ./server/build/install/mantis-masterv2-server/bin/* /apps/mantis/mantis-masterv2/bin/
COPY ./server/build/install/mantis-masterv2-server/lib/* /apps/mantis/mantis-masterv2/lib/

COPY ./server/src/main/resources/master-docker.properties /apps/mantis/mantis-masterv2/conf/

RUN mkdir -p /apps/mantis/mantis-masterv2/src/main/webapp
RUN mkdir -p /apps/mantis/mantis-masterv2/logs
RUN mkdir -p /tmp/MantisSpool/namedJobs
RUN mkdir -p /tmp/MantisArchive

WORKDIR /apps/mantis/mantis-masterv2

ENTRYPOINT [ "bin/mantis-masterv2-server", "-p", "conf/master-docker.properties" ]