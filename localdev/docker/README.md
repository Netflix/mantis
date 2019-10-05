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

3. Build Docker image for Mantis API

Clone the Mantis API project:
```bash
$ git clone git@github.com:netflix/mantis-api.git
```
```bash
$ cd mantis-api/
$ ./buildDockerImage.sh
```



4. Start the Mantis cluster with following command
```bash
cd <path to mantis repo>
docker-compose -f docker-compose-new-master.yml up
```
This starts up the following Docker containers:
- Zookeeper
- Mesos Master
- Mantis Master
- Mantis API
- Mesos Slave and Mantis Worker run on a single container (mantisagent)

5. Create and submit the sine-function Job Cluster
```bash
curl -X POST http://127.0.0.1:8100/api/namedjob/create --silent --data @$MANTIS_INSTALL_DIR/localdev/conf/namedJob-template -vvv

curl -X POST http://127.0.0.1:8100/api/submit --silent --data @$MANTIS_INSTALL_DIR/localdev/conf/submitJob-template -vvv
```

6. To get a shell on a running container:
```bash
docker exec -it mantisoss_mantisagent_1 bash
```
7. Try out the Mantis UI

Clone the Mantis UI project:
```bash
$ git clone git@github.com:netflix/mantis-ui.git
```

Run the following commands (in the root directory of this project) to get all dependencies installed and to start the server:
```
yarn
yarn serve
```
Once the node server is up it should print something like

```
 App running at:
 Local:   http://localhost:8080/
```

Point your browser to the URL and fill out the Registration form

1. Name : <Any valid string>
2. Email : <Any valid email>
3. Master Name : <Any Valid String>
4. Mantis Master API URL : http://localhost:7101
5. Mesos URL : http://localhost:5050

Hit Create

The screen would be initially empty.

Create a new Job Cluster

Click on the `Create New Job Cluster` button on the top right.

1. Specify the cluster name as `Sine-Function`

2. Click on Upload file and drag and drop the these two files
`sine-function/build/distributions/mantis-examples-sine-function-0.1.0-SNAPSHOT.zip`
`sine-function/build/distributions/mantis-examples-sine-function-0.1.0-SNAPSHOT.json`

3. Under the section `Stage 1 - Scheduling Information` click on `Edit` and reduce the Disk and Network requirements
for this worker to `10` and `12` respectively.

4. Under the section `Parameters` click on `Override Defaults`
Here you can override values for Job parameters. Let us override the parameter `useRandom` to `true`

Hit `Create Job Cluster`

5. Now let us submit a Job for our Job Cluster
Hit the `Submit` green button this will open up a submit screen that will allow you to override Resource configurations
as well as parameter values. Let us skip all that and directly hit the `Submit to Mantis` button on the bottom left.

6. View output of the job
If all goes well your job would go into 'Launched' state.

Scroll to the bottom and in the `Job Output` section click on `Start`

You should see output of the Sine function job being streamed below

```
Oct 4 2019, 03:55:39.338 PM - {"x": 26.000000, "y": 7.625585}
```

8. To teardown the Mantis cluster, issue the following command
```bash
cd <path to mantis repo>
docker-compose -f docker-compose-new-master.yml down
```
