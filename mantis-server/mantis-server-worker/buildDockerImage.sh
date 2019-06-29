#!/bin/bash

# build the mantis-server-worker fat jar
../../gradlew clean fatJar

# build the Docker image that packages the mantis-server-worker along with a running mesos-slave
docker build -t dev/mantisagent .

echo "Created Docker image 'dev/mantisagent'"
