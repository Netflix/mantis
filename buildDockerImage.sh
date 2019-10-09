#!/bin/bash

# build the mantis-master-akka executable
./gradlew clean installDist

# build the Docker image that packages mantis-control-plane
docker build -t dev/mantiscontrolplaneserver .

echo "Created Docker image 'dev/mantiscontrolplaneserver'"
