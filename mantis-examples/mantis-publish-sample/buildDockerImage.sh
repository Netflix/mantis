#!/bin/bash

# build the mantis-server-worker fat jar
../gradlew clean build shadowDistZip
unzip build/distributions/mantis-examples-mantis-publish-sample-shadow-0.1.0-SNAPSHOT.zip -d build/distributions 
# build the Docker image that packages the mantis-server-worker along with a running mesos-slave
docker build -t dev/mantispublish .

echo "Created Docker image 'dev/mantispublish'"
