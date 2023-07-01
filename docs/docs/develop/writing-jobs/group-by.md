# Writing Your Third Mantis Job: Group By / Aggregate

Until now we've run single stage Mantis jobs which a can run in a single process / container. Much of the power provided by Mantis is that we can design and implement a distributed job. Let's take a look at the [groupby-sample](https://github.com/Netflix/mantis/tree/master/mantis-examples/mantis-examples-groupby-sample) job definition and then break it down stage by stage.

```java

    @Override
    public Job<String> getJobInstance() {

        return MantisJob
            // Stream Request Events from our random data generator source
            .source(new ObservableSourceImpl<>(new RandomRequestSource()))
            .keyBy(x -> {
                if ("path".equalsIgnoreCase(groupByParam)) {
                    return x.getRequestPath();
                } else {
                    return x.getIpAddress();
                }
            })
            .window(WindowSpec.timed(Duration.ofSeconds(5)))
            .reduce(new ReduceFunction<RequestEvent, RequestAggregation>() {
                @Override
                public RequestAggregation initialValue() {
                    return RequestAggregation.builder().build();
                }

                @Override
                public RequestAggregation reduce(RequestAggregation acc, RequestEvent requestEvent) {
                    // TODO(hmittal): Need access to key-by key
                    return RequestAggregation.builder()
                        .path(requestEvent.getRequestPath())
                        .count(acc.getCount() + requestEvent.getLatency())
                        .build();
                }
            })
            .materialize()
            .keyBy(x -> "")
            .window(WindowSpec.timed(Duration.ofSeconds(5)))
            .reduce(new ReduceFunction<RequestAggregation, AggregationReport>() {
                @Override
                public AggregationReport initialValue() {
                    return new AggregationReport(new ConcurrentHashMap<>());
                }

                @Override
                public AggregationReport reduce(AggregationReport acc, RequestAggregation item) {
                    if (item != null && item.getPath() != null) {
                        acc.getPathToCountMap().put(item.getPath(), item.getCount());
                    }
                    return acc;
                }
            })
            .map(report -> {
                try {
                    return mapper.writeValueAsString(report);
                } catch (JsonProcessingException e) {
                    log.error(e.getMessage());
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .sink(new ObservableSinkImpl<>(Sinks.sysout()));
        return jobConfig
            .metadata(new Metadata.Builder()
                .name("GroupByPath")
                .description("Connects to a random data generator source"
                    + " and counts the number of requests for each uri within a window")
                .build())
            .create();
    }
```

The job definition above first applies a keyBy on path or ipaddress. This is followed by a parallel aggregation per key every 5 secs (window + reduce). These aggregations are happening in parallel and we need to collect the results together so we do another keyBy on empty key this time to collect records. This is executed only in 1 stage and 1 thread which collects these results in a single concurrent map every 5 sec. Notice we don't need to merge the contents of the map since the previous operator doesn't distribute one key across multiple nodes.

Physically, this job would create 4 stages:

1. `ScalarToGroupStage` -- reads from the source and applies `keyBy` path
2. `GroupToScalarStage` -- applies window and reduce on `RequestEvent` and  aggregates `requestEvent.getLatency()`
3. `ScalarToGroupStage` -- `keyBy` on empty key
4. `GroupToScalarStage` -- 1 thread, 1 worker stage that collects results into a single map and emit

### Old Implementation
If you find the new [DSL] limiting or hard to reason about wrt stages, please use old RxJava based interface. It's documentation moved to [legacy/distributed-group-by](../legacy/group-by) with sourcecode at
[WordCountJob.java](https://github.com/Netflix/mantis/blob/master/mantis-examples/mantis-examples-groupby-sample/src/main/java/com/netflix/mantis/samples/RequestAggregationJob.java).

A few callouts using the old approach are:

1. supports specifying concurrency param for each stage
2. supports custom (de)serialization formats in addition to java.io.Serializable

# Conclusion

We've now explored the concept of a multi-stage Mantis job which allows us to horizontally scale individual stages and express group by and aggregate semantics as a Mantis topology.
