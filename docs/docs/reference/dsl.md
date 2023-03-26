
Mantis leverages Reactive Streams for its Job [DSL], specifically RxJava 1.x.

Refer to the [RxJava 1.x Operator Guide](http://reactivex.io/documentation/operators.html) for more details.

Mantis has an new still-in-development operator DSL that could be used alternatively to limit dependence on RxJava 1.x specifically. Also, the new DSL has operators that are more in line with other data processing engines like apache-flink, apache-spark, etc.

# Mantis DSL

## Motivation
The primary motivation is to offer a simplified operator group that allows user to write mantis jobs not needing to understand two systems —— mantis's infrastructure overview and RxJava.

A secondary goal is to abstract RxJava interface away from the users and make it possible to upgrade to newer RxJava versions or even a custom implementation.

If you are coming from other data processing engines, the new DSL might look familiar. You can find the code in package [io.mantisrx.runtime.core](https://github.com/Netflix/mantis/tree/master/mantis-runtime/src/main/java/io/mantisrx/runtime/core). The primary interfaces are 
1. `MantisStream` —— primary interface for building data pipeline that has `filter`, `map`, `flatMap`, `keyBy`, `materialize`. [Github](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/core/MantisStream.java)
2. `KeyedMantisStream` —— It's keyed companion, that's used for streams partitioned by a key. In addition, it has `window` and `reduce`. [Github](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/core/KeyedMantisStream.java)


## Philosophy
MantisStream builds data processing as a directed-acyclic-graph (DAG) with operators like filter, map, flatMap stored as edges and data as nodes (vertices).
MantisStream and KeyedMantisStream store this graph which is processed into a MantisJob in the `sink()` method. This involves a topological sort on the graph that starts with source combined to a sink via one or more processing stages made up of DSL operators chained together.

## API

### MantisStream and KeyedMantisStream
MantisStream and KeyedMantisStream are defined as follows:
```
public interface MantisStream<T> {

    static <OUT> MantisStream<OUT> create(Context context) {
        return MantisStreamImpl.init();
    }

    <OUT> MantisStream<OUT> source(SourceFunction<OUT> sourceFunction);

    Config<T> sink(SinkFunction<T> sinkFunction);

    MantisStream<T> filter(FilterFunction<T> filterFn);
    <OUT> MantisStream<OUT> map(MapFunction<T, OUT> mapFn);
    <OUT> MantisStream<OUT> flatMap(FlatMapFunction<T, OUT> flatMapFn);

    MantisStream<T> materialize();

    <K> KeyedMantisStream<K, T> keyBy(KeyByFunction<K, T> keyFn);

}


public interface KeyedMantisStream<K, IN> {
    <OUT> KeyedMantisStream<K, OUT> map(MapFunction<IN, OUT> mapFn);

    <OUT> KeyedMantisStream<K, OUT> flatMap(FlatMapFunction<IN, OUT> flatMapFn);

    KeyedMantisStream<K, IN> filter(FilterFunction<IN> filterFn);

    // Must be present in every KeyedMantisStream because it's streaming data
    KeyedMantisStream<K, IN> window(WindowSpec spec);

    // Must immediately follow .window in how current implementation works
    <OUT> MantisStream<OUT> reduce(ReduceFunction<IN, OUT> reduceFn);
}
```

### Operators
The operators are interfaces that extend `MantisFunction`. This is done to allow writing operators as java-8 lambdas when they are single-abstract-methods classes.
Please find example operators right below.
Init and close would support custom initialization and freeing-up of resources before and after stream execution respectively.
```
public interface MantisFunction extends AutoCloseable {

    MantisFunction EMPTY = new MantisFunction() {};

    static MantisFunction empty() {
        return EMPTY;
    }

    default void init() {
    }

    @Override
    default void close() throws Exception {
    }
}
```

For example:

1. Map – transforms an element to another
    ``

        public interface MapFunction<IN, OUT> extends MantisFunction {
            OUT apply(IN in);
        }
    ``

2. FlatMapFunction — maps an element to zero, one, or more elements
    ``

        public interface FlatMapFunction<IN, OUT> extends MantisFunction {

            Iterable<OUT> apply(IN in);
        }
    ``

3. ReduceFunction — reduces a group of elements into a single element along with a single-abstract-method variant `SimpleReduceFunction`.
    ``

        public interface ReduceFunction<IN, OUT> extends MantisFunction {
            OUT initialValue();

            OUT reduce(OUT acc, IN in);
        }

        public interface SimpleReduceFunction<IN> extends ReduceFunction<IN, IN> {
            Object EMPTY = new Object();

            IN apply(IN acc, IN item);

            @Override
            default IN initialValue() {
                return (IN) EMPTY;
            }

            @Override
            default IN reduce(IN acc, IN item) {
                return (acc == EMPTY) ? item : apply(acc, item);
            }
        }
    ``

## Future Work
Here's a list of future work that needs to go into improving Mantis DSL

1. Supporting custom (de)serializers
   Existing MantisJob implement using stages supports custom (de)serializers via `StageConfig.codec()` method. The DSL implementation only supports java serialization for the time being.
2. Support a richer ProcessFunction than FlatMapFunction
   This will be equivalent to `Observable.lift` operator or apache-flink's `ProcessFunction` and `KeyedProcessFunction`
3. Source and Sink interfaces along with new implementations for existing sources and sinks
4. More purpose built operators for aggregations, broadcast, complex graph building, etc.