FROM docker-hub.netflix.net/tomcat:9.0-alpine

LABEL maintainer="mantis-oss-dev@netflix.com"

ADD build/libs/mantis-examples-mantis-publish-web-sample-0.1.0-SNAPSHOT.war /usr/local/tomcat/webapps/

EXPOSE 8080

CMD ["catalina.sh", "run"]
