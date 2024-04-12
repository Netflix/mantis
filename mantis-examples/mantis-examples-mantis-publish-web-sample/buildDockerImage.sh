#!/bin/bash

# build the mantis-server-worker fat jar
../gradlew clean build

# build the Docker image that packages the mantis-server-worker
docker build -t dev/mantispublishweb .

echo "Created Docker image 'dev/mantispublishweb'"
