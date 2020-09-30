# Alpine Linux with OpenJDK JRE
FROM openjdk:8-jre-alpine
# copy WAR into image

RUN mkdir -p /mnt/local/mantispublish
#RUN mkdir -p /mnt/local/mantispublish/mantis-examples-mantis-publish-sample-0.1.0-SNAPSHOT
#RUN mkdir -p /mnt/local/mantispublish/mantis-examples-mantis-publish-sample-0.1.0-SNAPSHOT/lib
#RUN mkdir -p /mnt/local/mantispublish/mantis-examples-mantis-publish-sample-0.1.0-SNAPSHOT/bin

# NOTE: Assumes you're building in the mantis-server-worker directory
COPY ./build/distributions/mantis-examples-mantis-publish-sample-shadow-0.1.0-SNAPSHOT/lib/mantis-examples-mantis-publish-sample-0.1.0-SNAPSHOT-all.jar  /mnt/local/mantispublish/
#COPY ./build/distributions/mantis-publish-netty-guice-shadow-1.3.0-SNAPSHOT.zip /mnt/local/mantispublish/
#RUN unzip /mnt/local/mantispublish/mantis-examples-mantis-publish-sample-0.1.0-SNAPSHOT.zip

WORKDIR /mnt/local/mantispublish/
# run application with this command line
#CMD ["./mantis-examples-mantis-publish-sample"]
CMD ["/usr/bin/java", "-jar", "/mnt/local/mantispublish/mantis-examples-mantis-publish-sample-0.1.0-SNAPSHOT-all.jar"]
#CMD ls -lah /mnt/local/mantispublish/mantis-examples-mantis-publish-sample-0.1.0-SNAPSHOT/bin
