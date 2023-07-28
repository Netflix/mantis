#!/bin/bash

# build the mantis-server-agent fat jar
../../gradlew clean installDist

# create sample job artifact to include in the image
../../gradlew :mantis-examples:mantis-examples-sine-function:mantisZipArtifact

mkdir build/distributions
cp ../../mantis-examples/mantis-examples-sine-function/build/distributions/* build/distributions/

# build the Docker image that packages the mantis-server-agent
docker build -t netflixoss/mantisagent .

echo "Created Docker image 'netflixoss/mantisagent'"
