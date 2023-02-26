#!/bin/bash

# create the mantisapi binary
./gradlew clean installDist

cp ../mantis-examples/twitter-sample/build/distributions/mantis-examples-twitter-sample-0.1.0-SNAPSHOT.zip .
cp ../mantis-examples/twitter-sample/build/distributions/mantis-examples-twitter-sample-0.1.0-SNAPSHOT.json .
cp ../mantis-examples/sine-function/build/distributions/mantis-examples-sine-function-0.1.0-SNAPSHOT.zip .
cp ../mantis-examples/sine-function/build/distributions/mantis-examples-sine-function-0.1.0-SNAPSHOT.json .
cp ../mantis-examples/groupby-sample/build/distributions/mantis-examples-groupby-sample-0.1.0-SNAPSHOT.zip .
cp ../mantis-examples/groupby-sample/build/distributions/mantis-examples-groupby-sample-0.1.0-SNAPSHOT.json .
cp ../mantis-examples/synthetic-sourcejob/build/distributions/mantis-examples-synthetic-sourcejob-0.1.0-SNAPSHOT.zip .
cp ../mantis-examples/synthetic-sourcejob/build/distributions/mantis-examples-synthetic-sourcejob-0.1.0-SNAPSHOT.json .
cp ../mantis-examples/jobconnector-sample/build/distributions/mantis-examples-jobconnector-sample-0.1.0-SNAPSHOT.zip .
cp ../mantis-examples/jobconnector-sample/build/distributions/mantis-examples-jobconnector-sample-0.1.0-SNAPSHOT.json .
cp ../mantis-examples/jobconnector-sample/build/distributions/mantis-examples-jobconnector-sample-0.1.0-SNAPSHOT.json .

cp ../mantis-source-jobs/kafka-source-job/build/distributions/mantis-source-job-kafka-1.3.0-SNAPSHOT.zip .
cp ../mantis-source-jobs/kafka-source-job/build/distributions/mantis-source-job-kafka-1.3.0-SNAPSHOT.json .

cp ../mantis-source-jobs/publish-source-job/build/distributions/mantis-source-job-publish-1.3.0-SNAPSHOT.json .
cp ../mantis-source-jobs/publish-source-job/build/distributions/mantis-source-job-publish-1.3.0-SNAPSHOT.zip .

# build the docker image

docker build -t dev/mantisapi .

echo "Created Docker image 'netflixoss/mantisapi'"
