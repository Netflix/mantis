# Running Mantis in Docker

> Install [Docker for Mac](https://docs.docker.com/docker-for-mac/install/)

1. Clone the Mantis repo:

```bash
$ git clone git@github.com:netflix/mantis.git
```

1. Build Docker image for Mantis Control Plane

Clone the Mantis Control Plane:

```bash
$ git clone git@github.com:netflix/mantis-control-plane.git
```

```bash
$ cd mantis-control-plane/
$ ./buildDockerImage.sh
```

2. Build Docker image for Mantis Worker

Clone the Mantis examples:

```bash
$ git clone git@github.com:netflix/mantis-examples.git
```

```bash
# Create a sine-function artifact
$ cd mantis-examples/
$ ./gradlew clean mantis-examples-sine-function:mantisZipArtifact

# Copy to conf/ that is mounted on the mantis worker Docker image
$ cp sine-function/build/distributions/mantis-examples-sine-function-0.1.0-SNAPSHOT.zip <path to mantis repo>/localdev/conf/

# Build the Worker docker image
$ cd <path to mantis repo>/mantis-server/mantis-server-worker
./buildDockerImage.sh
```

3. Start the Mantis cluster with following command
```bash
cd <path to mantis repo>
docker-compose -f docker-compose-new-master.yml up
```
This starts up the following Docker containers:
- Zookeeper
- Mesos Master
- Mantis Master
- Mesos Slave and Mantis Worker run on a single container (mantisagent)

4. Create and submit the sine-function Job Cluster
```bash
curl -X POST http://127.0.0.1:8100/api/namedjob/create --silent --data @$MANTIS_INSTALL_DIR/mantis-localdev/conf/namedJob-template -vvv

curl -X POST http://127.0.0.1:8100/api/submit --silent --data @$MANTIS_INSTALL_DIR/mantis-localdev/conf/submitJob-template -vvv
```

4. To get a shell on a running container:
```bash
docker exec -it mantisoss_mantisagent_1 bash
```

5. To teardown the Mantis cluster, issue the following command
```bash
cd <path to mantis repo>
docker-compose -f docker-compose-new-master.yml down
```
