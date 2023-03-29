# [Legacy, RxJava] Mantis Job - Group By / Aggregate

!!! note

    There is an alternate implementation that allows writing a Mantis Job as a series of operators operating directly on a `MantisStream` instance which abstracts information about RxJava, Observables from the user and offers a simpler way (hopefully 🤞) to write mantis jobs. Please see [Mantis DSL docs](../../../../reference/dsl) for more details or [this documentation](../../group-by) for the sample job.


Until now we've run single stage Mantis jobs which a can run in a single process / container. Much of the power provided by Mantis is that we can design and implement a distributed job. Let's take a look at the [groupby-sample](https://github.com/Netflix/mantis/tree/master/mantis-examples/mantis-examples-groupby-sample) job definition and then break it down stage by stage.

```java

    @Override
    public Job<String> getJobInstance() {

        return MantisJob
                // Stream Request Events from our random data generator source
                .source(new RandomRequestSource())

                // Groups requests by path
                .stage(new GroupByStage(), GroupByStage.config())

                // Computes count per path over a window
                .stage(new AggregationStage(), AggregationStage.config())

                // Collects the data and makes it availabe over SSE
                .stage(new CollectStage(), CollectStage.config())

                // Reuse built in sink that eagerly subscribes and delivers data over SSE
                .sink(Sinks.eagerSubscribe(
                        Sinks.sse((String data) -> data)))

                .metadata(new Metadata.Builder()
                        .name("GroupByPath")
                        .description("Connects to a random data generator source"
                                + " and counts the number of requests for each uri within a window")
                        .build())
                .create();

    }
```

The job definition above should look relatively familiar with the exception of the fact that our job has three stages. These stages and their configurations are all relatively simple but take advantage of the scalability of Mantis when used in conjunction with each other;

* The `GroupByStage` will group events according to some user specified criteria.
* The `AggregationStage` will perform an aggregation on a per-group basis.
* The `CollectStage` will collect all aggregations and create a report.

Let's explore each of these stages in sequence.

## Stage 1: Group By Stage

The GroupByStage implements `ToGroupComputation<RequestEvent, String, RequestEvent>` which tells Mantis that the `call` method will be returning `MantisGroup<String, RequestEvent>` which represents a group key and the data. You may recall in the previous tutorials we used the reactive `groupBy` operator to group data. The reactive operator performs an in-memory group by which has some scaling limitations. Grouping data using a Mantis stage does not have the same limitations and allows us to scale the group by operation across multiple containers.

The `config()` method also changes to specify this different stage type. It also allows us to specify `concurrentInput()` on the config allowing the `call` method to be run concurrently in this container. Also note the result of `getParameters()` is added to the config via the `withParameters` method, it isn't an interface method for stages but we're specifying parameters this way for convenience.

```java
public class GroupByStage implements ToGroupComputation<RequestEvent, String, RequestEvent> {

    private static final String GROUPBY_FIELD_PARAM = "groupByField";
    private boolean groupByPath = true;
    @Override
    public Observable<MantisGroup<String, RequestEvent>> call(Context context, Observable<RequestEvent> requestEventO) {
        return requestEventO
                .map((requestEvent) -> {
                    if(groupByPath) {
                        return new MantisGroup<>(requestEvent.getRequestPath(), requestEvent);
                    } else {
                        return new MantisGroup<>(requestEvent.getIpAddress(), requestEvent);
                    }
                });
    }

    @Override
    public void init(Context context) {
        String groupByField = (String)context.getParameters().get(GROUPBY_FIELD_PARAM,"path");
        groupByPath = groupByField.equalsIgnoreCase("path") ? true : false;
    }

    /**
     * Here we declare stage specific parameters.
     * @return
     */
    public static List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = new ArrayList<>();

        // Group by field
        params.add(new StringParameter()
                .name(GROUPBY_FIELD_PARAM)
                .description("The key to group events by")
                .validator(Validators.notNullOrEmpty())
                .defaultValue("path")
                .build())	;

        return params;
    }

    public static ScalarToGroup.Config<RequestEvent, String, RequestEvent> config(){
        return new ScalarToGroup.Config<RequestEvent, String, RequestEvent>()
                .description("Group event data by path/ip")
                .concurrentInput() // signifies events can be processed in parallel
                .withParameters(getParameters())
                .codec(RequestEvent.requestEventCodec());
    }
}
```

We should note that this stage is horizontally scalable. We can run as many of these as necessary to handle the inflow of data which is how Mantis allows us to run a scalable group by operation.

## Stage 2: Aggregation Stage

The previous stage produces `MantisGroup<String, RequestEvent>` by implementing `ToGroupComputation`. If we think of `ToGroupComputation` producing groups of data we can think of `GroupToScalarComputation<K, T, R>` as the inverse computing a scalar value from a group.

The `init()`, `config()` and `getParameters()` methods should be familiar by now but let's take a closer look at the `call()` method. We're performing the same operation the stream as we did in the first two tutorials. Notice how we still group by `MantisGroup::getKeyValue`. This is because while Mantis guarantees that all data for an individual group from the previous stage will land on the same container in this stage, it does not invoke the `call()` method for each individual group and thus we need to handle the groups ourselves.

```java
@Slf4j
public class AggregationStage implements GroupToScalarComputation<String, RequestEvent, RequestAggregation {

    public static final String AGGREGATION_DURATION_MSEC_PARAM = "AggregationDurationMsec";
    int aggregationDurationMsec;

    /**
     * The call method is invoked by the Mantis runtime while executing the job.
     * @param context Provides metadata information related to the current job.
     * @param mantisGroupO This is an Observable of {@link MantisGroup} events. Each event is a pair of the Key -> uri Path and
     *                     the {@link RequestEvent} event itself.
     * @return
     */
    @Override
    public Observable<RequestAggregation> call(Context context, Observable<MantisGroup<String, RequestEvent>> mantisGroupO) {
        return mantisGroupO
                .window(aggregationDurationMsec, TimeUnit.MILLISECONDS)
                .flatMap((omg) -> omg.groupBy(MantisGroup::getKeyValue)
                        .flatMap((go) -> go.reduce(0, (accumulator, value) ->  accumulator = accumulator + 1)
                                .map((count) -> RequestAggregation.builder().count(count).path(go.getKey()).build())
                                .doOnNext((aggregate) -> {
                                    log.debug("Generated aggregate {}", aggregate);
                                })
                        ));
    }

    /**
     * Invoked only once during job startup. A good place to add one time initialization actions.
     * @param context
     */
    @Override
    public void init(Context context) {
        aggregationDurationMsec = (int)context.getParameters().get(AGGREGATION_DURATION_MSEC_PARAM, 1000);
    }

    /**
     * Provides the Mantis runtime configuration information about the type of computation done by this stage.
     * E.g in this case it specifies this is a GroupToScalar computation and also provides a {@link Codec} on how to
     * serialize the {@link RequestAggregation} events before sending it to the {@link CollectStage}
     * @return
     */
    public static GroupToScalar.Config<String, RequestEvent, RequestAggregation> config(){
        return new GroupToScalar.Config<String, RequestEvent,RequestAggregation>()
                .description("sum events for a path")
                .codec(RequestAggregation.requestAggregationCodec())
                .withParameters(getParameters());
    }

    /**
     * Here we declare stage specific parameters.
     * @return
     */
    public static List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = new ArrayList<>();

        // Aggregation duration
        params.add(new IntParameter()
                .name(AGGREGATION_DURATION_MSEC_PARAM)
                .description("window size for aggregation")
                .validator(Validators.range(100, 10000))
                .defaultValue(5000)
                .build())	;

        return params;
    }

}
```

Much like the previous stage this stage is also horizontally scalable up to the cardinality of our group by. Allowing us to individually (or automatically via autoscaling) scale these two stages ensures this job can be correctly sized for the workload.

## Stage 3: Collect Stage

The third stage collects data from all of the upstream workers and generates a report. We can see in the code below the stream is windowed for five seconds, then we flatmap a reduce over the window and invoke the `RequestAggregationAccumulator#generateReport()` method on the reduced value.

```java
@Slf4j
public class CollectStage implements ScalarComputation<RequestAggregation,String> {
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Observable<String> call(Context context, Observable<RequestAggregation> requestAggregationO) {
        return requestAggregationO
                .window(5, TimeUnit.SECONDS)
                .flatMap((requestAggO) -> requestAggO
                        .reduce(new RequestAggregationAccumulator(),(acc, requestAgg) -> acc.addAggregation(requestAgg))
                        .map(RequestAggregationAccumulator::generateReport)
                        .doOnNext((report) -> {
                            log.debug("Generated Collection report {}", report);
                        })
                )
                .map((report) -> {
                    try {
                        return mapper.writeValueAsString(report);
                    } catch (JsonProcessingException e) {
                        log.error(e.getMessage());
                        return null;
                    }
                }).filter(Objects::nonNull);
    }

    @Override
    public void init(Context context) {

    }

    public static ScalarToScalar.Config<RequestAggregation,String> config(){
        return new ScalarToScalar.Config<RequestAggregation,String>()
                .codec(Codecs.string());
    }

    /**
     * The accumulator class as the name suggests accumulates all aggregates seen during a window and
     * generates a consolidated report at the end.
     */
    static class RequestAggregationAccumulator {
        private final Map<String, Integer> pathToCountMap = new HashMap<>();

        public RequestAggregationAccumulator()  {}

        public RequestAggregationAccumulator addAggregation(RequestAggregation agg) {
            pathToCountMap.put(agg.getPath(), agg.getCount());
            return this;
        }

        public AggregationReport generateReport() {
            log.info("Generated report from=> {}", pathToCountMap);
            return new AggregationReport(pathToCountMap);
        }
    }
}
```

Unlike the previous stages we only run a single collect stage which gathers all the data in five second batches and generates a report to be output on the sink.

# Conclusion

We've now explored the concept of a multi-stage Mantis job which allows us to horizontally scale individual stages and express group by and aggregate semantics as a Mantis topology.
