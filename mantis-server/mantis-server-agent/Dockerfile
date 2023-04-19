FROM azul/zulu-openjdk:8-latest

MAINTAINER Mantis Developers <mantis-oss-dev@netflix.com>

COPY ./build/install/mantis-server-agent/bin/* /apps/mantis/mantis-server-agent/bin/
COPY ./build/install/mantis-server-agent/lib/* /apps/mantis/mantis-server-agent/lib/

COPY ./src/main/resources/agent-docker.properties /apps/mantis/mantis-server-agent/conf/

RUN mkdir -p /apps/mantis/mantis-server-agent/src/main/webapp
RUN mkdir -p /apps/mantis/mantis-server-agent/logs

WORKDIR /apps/mantis/mantis-server-agent

ENTRYPOINT [ "bin/mantis-server-agent", "-p", "conf/agent-docker.properties" ]
