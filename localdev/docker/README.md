# Running Mantis in Docker

> Install [Docker for Mac](https://docs.docker.com/docker-for-mac/install/)

> For running the new Mantis Master, build the docker image from new mantis master repo and execute docker-compose-new-master.yml instead

1. Build Docker image for Mantis Master

```bash
export MANTIS_INSTALL_DIR=<path to Mantis>
cd $MANTIS_INSTALL_DIR/mantis-server/mantis-server-master
./buildDockerImage.sh
```

2. Build Docker image for Mantis Worker
```bash
# Create a sine-function artifact
cd $MANTIS_INSTALL_DIR/mantis-sdk/examples/sine-function
../../../gradlew clean mantisZipArtifact -Pversion=1.0

# Copy to conf/ that is mounted on the mantis worker Docker image
$MANTIS_INSTALL_DIR/mantis-sdk/examples/sine-function# cp build/distributions/sine-function-1.0.zip ../../../mantis-localdev/conf

# Build the Worker docker image
cd $MANTIS_INSTALL_DIR/mantis-server/mantis-server-worker
./buildDockerImage.sh
```

3. Start the Mantis cluster with following command
```bash
cd $MANTIS_INSTALL_DIR
docker-compose -f docker-compose.yml up
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
cd <path to Mantis>
docker-compose -f docker-compose.yml down
```
