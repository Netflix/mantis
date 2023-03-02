FROM java:8
MAINTAINER Mantis Developers <mantis-oss-dev@netflix.com>

COPY ./build/install/mantis-api/bin/* /apps/nfmantisapi/bin/
COPY ./build/install/mantis-api/lib/* /apps/nfmantisapi/lib/

COPY ./conf/local.properties /apps/nfmantisapi/conf/

RUN mkdir -p /apps/nfmantisapi/mantisArtifacts
RUN mkdir -p /logs/mantisapi

# OPTIONAL - To bootstrap mantis api with job artifacts. Assumes mantis-examples have been built.
# copy examples
COPY ./mantis-examples-twitter-sample-0.1.0-SNAPSHOT.zip /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-twitter-sample-0.1.0-SNAPSHOT.json /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-sine-function-0.1.0-SNAPSHOT.zip /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-sine-function-0.1.0-SNAPSHOT.json /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-groupby-sample-0.1.0-SNAPSHOT.zip /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-groupby-sample-0.1.0-SNAPSHOT.json /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-synthetic-sourcejob-0.1.0-SNAPSHOT.zip /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-synthetic-sourcejob-0.1.0-SNAPSHOT.json /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-jobconnector-sample-0.1.0-SNAPSHOT.zip /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-examples-jobconnector-sample-0.1.0-SNAPSHOT.json /apps/nfmantisapi/mantisArtifacts/

# copy source jobs
COPY ./mantis-source-job-kafka-1.3.0-SNAPSHOT.zip /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-source-job-kafka-1.3.0-SNAPSHOT.json /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-source-job-publish-1.3.0-SNAPSHOT.zip /apps/nfmantisapi/mantisArtifacts/
COPY ./mantis-source-job-publish-1.3.0-SNAPSHOT.json /apps/nfmantisapi/mantisArtifacts/

WORKDIR /apps/nfmantisapi

ENTRYPOINT [ "bin/mantis-api", "-p", "conf/local.properties" ]
