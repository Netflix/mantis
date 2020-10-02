# <img alt="Mantis logo" src="./.assets/mantis.png" width="200">

[![Build Status](https://img.shields.io/travis/com/Netflix/mantis.svg)](https://travis-ci.com/Netflix/mantis)
[![OSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/mantis.svg)](https://github.com/Netflix/mantis)
[![License](https://img.shields.io/github/license/Netflix/mantis.svg)](https://www.apache.org/licenses/LICENSE-2.0)

## Mantis Documentation

[https://netflix.github.io/mantis/](https://netflix.github.io/mantis/) 

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
