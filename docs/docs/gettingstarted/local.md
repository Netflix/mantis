# Explore a Mantis Job locally

## Prerequisites

JDK 8 or higher

## Build and run the synthetic-sourcejob sample

Clone the mantis-examples repo

```bash
$ git clone https://github.com/Netflix/mantis-examples.git
```

Run the synthetic-sourcejob sample via gradle

```bash
$ cd mantis-examples/synthetic-sourcejob
$ ../gradlew execute
```

```bash
This will launch the job and you would see output like
2019-10-06 14:14:07 INFO  StageExecutors:254 main - initializing io.mantisrx.sourcejob.synthetic.stage.TaggingStage
2019-10-06 14:14:07 INFO  SinkPublisher:82 main - Got sink subscription, onSubscribe=null
2019-10-06 14:14:07 INFO  ServerSentEventsSink:141 main - Serving modern HTTP SSE server sink on port: 8436
```

The default Mantis sink is a ServerSentEvent sink that opens a port allowing anyone to connect
to it and stream the results of the job.
Look for a line like
```bash
Serving modern HTTP SSE server sink on port: 8436
```
The source job is now up and ready to serve data.
This job streams request events sourced from an imaginary service. The RequestEvent data
has information such as uri, status, userId, country etc.

Let us find requests from countries where the status code is 500
Such a MQL query would look like this
```bash
select country from stream where status==500
```
In another terminal window curl this port
```bash
$ curl "localhost:8436?subscriptionId=nj&criterion=select%20country%20from%20stream%20where%20status%3D%3D500&clientId=nj2"
```

You should see events matching your query appear in your terminal

```bash
data: {"country":"Ecuador","mantis.meta.sourceName":"SyntheticRequestSource","mantis.meta.timestamp":1570396602599,"status":500}

data: {"country":"Solomon Islands","mantis.meta.sourceName":"SyntheticRequestSource","mantis.meta.timestamp":1570396603342,"status":500}

data: {"country":"Liberia","mantis.meta.sourceName":"SyntheticRequestSource","mantis.meta.timestamp":1570396603844,"status":500}
```

Next. Import the project into your IDE to explore the code.

You can see more examples under the [Mantis examples repository](https://github.com/netflix/mantis-examples)
