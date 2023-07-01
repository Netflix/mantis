# Writing Your First Mantis Job
We'll be doing the classic word count example for streaming data for the tutorial section. For this example we'll be keeping it simple and focusing on the processing logic and job provider. The tutorials are structured progressively to allow us to incrementally build some experience writing jobs without getting overwhelmed with details. We'll stream text from a Project Gutenberg book, perform some application logic on the stream, and then write the data to a sink for consumption by other Mantis jobs. If you want to follow along, please check the [Word Count](https://github.com/Netflix/mantis/tree/master/mantis-examples/mantis-examples-wordcount) module in Mantis repository.

There are a few things to keep in mind when implementing a Mantis Job;

* We're just writing Java and there are a few interfaces necessary for Mantis
* Mantis jobs are composed of a source, n stages, and a sink.
* Mantis makes heavy use of Reactive Streams as a DSL for implementing processing logic.


## WordCountJob

The full source of the [WordCountDslJob](https://github.com/Netflix/mantis/blob/master/mantis-examples/mantis-examples-wordcount/src/main/java/com/netflix/mantis/examples/wordcount/WordCountDslJob.java) class is included below with imports elided. This class implements the [`io.mantisrx.runtime.MantisJobProvider`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/MantisJobProvider.java) interface which the Mantis runtime loads. `MantisJobProvider#getJobInstance()` provides the runtime with an entry point to your job's code.

```java
/**
 * This sample demonstrates ingesting data from a text file and counting the number of occurrences of words within a 10
 * sec hopping window.
 * Run the main method of this class and then look for a the SSE port in the output
 * E.g
 * <code> Serving modern HTTP SSE server sink on port: 8650 </code>
 * You can curl this port <code> curl localhost:8650</code> to view the output of the job.
 */
@Slf4j
public class WordCountDslJob extends MantisJobProvider<String> {

    @Override
    public Job<String> getJobInstance() {
        return MantisStream.create(null)
            .source(new ObservableSourceImpl<>(new IlliadSource()))
            .flatMap(this::tokenize)
            .map(x -> {
                // this guards against TooLongFrameException for some reason, need to investigate!
                try {
                    Thread.sleep(0, 10000);
                } catch (InterruptedException ignored) {
                }
                return x;
            })
            .keyBy(WordCountPair::getWord)
            .window(WindowSpec.timed(Duration.ofSeconds(10)))
            .reduce((ReduceFunctionImpl<WordCountPair>) (acc, item) -> {
                if (acc.getWord() != null && !acc.getWord().isEmpty() && !acc.getWord().equals(item.getWord())) {
                    log.warn("keys dont match: acc ({}) vs item ({})", acc.getWord(), item.getWord());
                }
                return new WordCountPair(acc.getWord(), acc.getCount() + item.getCount());
            })
            .map(WordCountPair::toString)
            // Reuse built in sink that eagerly subscribes and delivers data over SSE
            .sink(new ObservableSinkImpl<>(Sinks.eagerSubscribe(Sinks.sse((String data) -> data))))
            .metadata(new Metadata.Builder()
                .name("WordCount")
                .description("Reads Homer's The Illiad faster than we can.")
                .build())
            .create();
    }

    private List<WordCountPair> tokenize(String text) {
        StringTokenizer tokenizer = new StringTokenizer(text);
        List<WordCountPair> wordCountPairs = new ArrayList<>();
        while(tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
            wordCountPairs.add(new WordCountPair(word,1));
        }
        return wordCountPairs;
    }


    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new WordCountDslJob().getJobInstance());
    }
}

```

There are several things going on here, let's examine them one at a time...

### The Source

We specify our source in the line `.source(new ObservableSourceImpl<>(new IlliadSource()))`. The source handles data ingestion and it is very common to use a pre-existing parameterized source when writing jobs. Mantis provides several sources which handle managing connections and queries to other jobs. The newer DSL api allows reusing existing sources and sinks with this custom `ObservableSourceImpl` wrapper that's unboxed during job graph building phase.

In the next tutorial we'll learn how to implement our own source which ingests data from Twitter.

### Operators

This is the biggest change from mantis job author's perspective. We are abstracting the older stage model (with RxJava Observable API) with new operators that do item processing. For this example, the job is implemented using 2 stages——ScalarToGroup and GroupToScalar with `keyBy` performed as a distributed `keyby`. Please see the next tutorial to learn more about distributed job and keyBy.

We'll take advantage of Java's lambda syntax to implement the operators.

The operators are chained together if they can be processed in the same worker and use java serialization to (de)serialize events for inter worker communication (need to implement java.io.Serializable).

### Sink
The sink runs a web server that listens for connection requests from downstream instances and send Server-Sent-Events (SSE) events. Similar to source, new dsl uses a wrapper `ObservableSinkImpl` to support reusing existing sink implementations.

### The Job Provider

The [`MantisJobProvider`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/MantisJobProvider.java) interface is what the Mantis runtime expects to load. The runtime reads [`resources/META-INF/services/io.mantisrx.runtime.MantisJobProvider`](https://github.com/Netflix/mantis/blob/master/mantis-examples/mantis-examples-wordcount/src/main/resources/META-INF/services/io.mantisrx.runtime.MantisJobProvider) to discover the fully qualified classname of the MantisJobProvider to be used as an entry point for the application.

### Main Method

The main method invokes the [`LocalJobExecutorNetworked`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/executor/LocalJobExecutorNetworked.java) `execute` method to run our job locally. The first three tutorials will take advantage of the ability to run jobs locally. In the fourth tutorial we will explore uploading and submitting our job on a Mantis cloud deployment for greater scalability. We can and should run this main method by following commands from the Mantis repository:
```bash
$ cd mantis-examples/mantis-examples-wordcount
$ ../../gradlew execute
```

```java
    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new WordCountJob().getJobInstance());
    }

```

### Old Implementation
If you find the new [DSL] limiting, please use old RxJava based interface. It's documentation moved to [legacy/WordCount](../legacy/word-count) with sourcecode at
[WordCountJob.java](https://github.com/Netflix/mantis/blob/master/mantis-examples/mantis-examples-wordcount/src/main/java/com/netflix/mantis/examples/wordcount/WordCountJob.java).

A few callouts using the old approach are:

1. keyBy is not distributed and is local to the worker, pl create a new ScalarToGroupStage (example 3) to process in distributed stage.
2. supports specifying concurrency param for each stage
3. also supports custom (de)serialization formats in addition to java.io.Serializable

## Conclusions and Future Work

We've implemented a complete end-to-end Mantis job which counts words from The Illiad repeatedly. This leaves much to be desired. If you inspect our source we're really just iterating over the same data set every ten seconds. In the next tutorial we'll explore the task of writing our own custom source to pull external data from Twitter into Mantis and designing this source in a templated fashion so that it can be used with different queries and API keys.

As an extra credit task see if you can modify the job to print the top 10 words instead of the entire list.
