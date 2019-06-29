#!/usr/bin/env bash
echo "Building Mantis Master Docker Image..."
cd ../mantis-server/mantis-server-master && ./buildDockerImage.sh

echo "Building Sine Function Job..."
cd ../../mantis-sdk/examples/sine-function
../../../gradlew clean mantisZipArtifact -Pversion=1.0
cp build/distributions/sine-function-1.0.zip ../../../mantis-localdev/conf

echo "Building Manits Worker Docker Image..."
cd ../../../mantis-server/mantis-server-worker && ./buildDockerImage.sh
cd ../../
