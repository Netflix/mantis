#!/bin/bash
# This script will build the project.

if [ "$TRAVIS_PULL_REQUEST" != "false" ] || [ "$GRADLE_PUBLISH" == "false" ]; then
  echo -e "Build Pull Request #$TRAVIS_PULL_REQUEST => Branch [$TRAVIS_BRANCH]"
  ./gradlew build --stacktrace
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ] && [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e 'Build master with Snapshot'
  ./gradlew -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" build snapshot --stacktrace
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ]; then
  echo -e 'Build Branch => Branch ['$TRAVIS_BRANCH']'
  ./gradlew build --stacktrace
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ]; then
  echo -e 'Build Branch for Release => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']'
  case "$TRAVIS_TAG" in
  *-rc\.*)
    ./gradlew -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" candidate --stacktrace
    ;;
  *)
    ./gradlew -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" final --stacktrace
    ;;
  esac
else
  echo -e 'WARN: Should not be here => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']  Pull Request ['$TRAVIS_PULL_REQUEST']'
  ./gradlew build --stacktrace
fi
