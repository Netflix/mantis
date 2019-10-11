# <img alt="Mantis logo" src="./.assets/mantis.png" width="200">

[![Build Status](https://img.shields.io/travis/com/Netflix/mantis.svg)](https://travis-ci.com/Netflix/mantis)
[![OSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/mantis.svg)](https://github.com/Netflix/mantis)
[![License](https://img.shields.io/github/license/Netflix/mantis.svg)](https://www.apache.org/licenses/LICENSE-2.0)

## Mantis Components

1. [Runtime (this repo)](https://github.com/netflix/mantis)
1. [Control Plane](https://github.com/netflix/mantis-control-plane)
1. [Publisher Client](https://github.com/netflix/mantis-publish)
1. [Source Jobs](https://github.com/netflix/mantis-source-jobs)
1. [Connectors](https://github.com/netflix/mantis-connectors)
1. [MQL](https://github.com/netflix/mantis-mql)
1. [API](https://github.com/netflix/mantis-api)
1. [CLI](https://github.com/netflix/mantis-cli)
1. [UI](https://github.com/netflix/mantis-ui)
1. [Gradle Plugin](https://github.com/netflix/mantis-gradle-plugin)
1. [Examples](https://github.com/netflix/mantis-examples)

## Development

### Building

```sh
$ ./gradlew clean build
```

### Testing

```sh
$ ./gradlew clean test
```

### Building deployment into local Maven cache

```sh
$ ./gradlew clean publishNebulaPublicationToMavenLocal
```

### Releasing

We release by tagging which kicks off a CI build. The CI build will run tests, integration tests,
static analysis, checkstyle, build, and then publish to the public Bintray repo to be synced into Maven Central.

Tag format:

```
vMajor.Minor.Patch
```

You can tag via git or through Github's Release UI.

## Contributing

Mantis is interested in building the community. We welcome any forms of contributions through discussions on any
of our [mailing lists](https://netflix.github.io/mantis/community/#mailing-lists) or through patches.

For more information on contribution, check out the contributions file here:

- [https://github.com/Netflix/mantis/blob/master/CONTRIBUTING.md](https://github.com/Netflix/mantis/blob/master/CONTRIBUTING.md)
