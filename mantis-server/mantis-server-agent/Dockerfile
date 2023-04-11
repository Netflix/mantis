FROM azul/zulu-openjdk:8u345

MAINTAINER Mantis Developers <mantis-oss-dev@netflix.com>

COPY ./mantis-server-agent/build/install/mantis-control-plane-server/bin/* /apps/mantis/mantis-server-agent/bin/
COPY ./mantis-server-agent/build/install/mantis-control-plane-server/lib/* /apps/mantis/mantis-server-agent/lib/

COPY ./mantis-server-agent/src/main/resources/agent-docker.properties /apps/mantis/mantis-server-agent/conf/

RUN mkdir -p /apps/mantis/mantis-server-agent/src/main/webapp
RUN mkdir -p /apps/mantis/mantis-server-agent/logs

WORKDIR /apps/mantis/mantis-server-agent

ENTRYPOINT [ "bin/mantis-server-agent", "-p", "conf/agent-docker.properties" ]
