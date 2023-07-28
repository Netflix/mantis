#!/bin/bash

# build the mantis-server-worker fat jar
../../gradlew clean installDist
../../gradlew :mantis-examples:mantis-examples-sine-function:mantisZipArtifact

mkdir build/distributions
cp ../../mantis-examples/mantis-examples-sine-function/build/distributions/* build/distributions/

# build the Docker image that packages the mantis-server-worker along with a running mesos-slave
docker build -t netflixoss/mantisagent .

echo "Created Docker image 'netflixoss/mantisagent'"
