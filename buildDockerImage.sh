#!/bin/bash

# build the mantis-master-akka executable
./gradlew clean installDist

# build the Docker image that packages the mantis-control-plane

docker build -t dev/mantiscontrolplaneserver .
echo "Created Docker image 'dev/mantiscontrolplaneserver'"

