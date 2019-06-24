#!/bin/bash

# build the mantis-master-akka executable
./gradlew clean installDist

# build the Docker image that packages the mantis-master-akka
docker build -t dev/mantismaster2 .

echo "Created Docker image 'dev/mantismaster2'"

