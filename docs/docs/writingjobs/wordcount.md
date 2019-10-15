# Writing Your First Mantis Job
We'll be doing the classic word count example for streaming data for the tutorial section. For this example we'll be keeping it simple and focusing on the processing logic and job provider. The tutorials are structured progressively to allow us to incrementally build some experience writing jobs without getting overwhelmed with details. We'll stream text from a Project Gutenberg book, perform some application logic on the stream, and then write the data to a sink for consumption by other Mantis jobs. If you want to follow along check out the [Word Count](https://github.com/Netflix/mantis-examples/tree/master/wordcount) project in our [mantis-examples](https://github.com/Netflix/mantis-examples/) repository.

There are a few things to keep in mind when implementing a Mantis Job;

* We're just writing Java and there are a few interfaces necessary for Mantis
* Mantis jobs are composed of a source, n stages, and a sink.
* Mantis makes heavy use of Reactive Streams as a DSL for implementing processing logic.


## WordCountJob

The full source of the [WordCountJob](https://github.com/Netflix/mantis-examples/wordcount/...) class is included below with imports elided. This class implements the [`io.mantisrx.runtime.MantisJobProvider`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/MantisJobProvider.java) interface which the Mantis runtime loads. `MantisJobProvider#getJobInstance()` provides the runtime with an entry point to your job's code.

```java
/**
 * This sample demonstrates ingesting data from a text file and counting the number of occurrences of words within a 10
 * sec hopping window.
 * Run the main method of this class and then look for a the SSE port in the output
 * E.g
 * <code> Serving modern HTTP SSE server sink on port: 8650 </code>
 * You can curl this port <code> curl localhost:8650</code> to view the output of the job.
 *
 * To run via gradle
 * /gradlew :mantis-examples-wordcount:execute
 */
@Slf4j
public class WordCountJob extends MantisJobProvider<String> {

    @Override
    public Job<String> getJobInstance() {
        return MantisJob
                .source(new IlliadSource()) // Ignore for now, we'll implement one in the next tutorial.
                .stage((context, dataO) -> dataO
                        // Tokenize
                        .flatMap((text) -> Observable.from(tokenize(text)))
                        // On a hopping window of 10 seconds
                        .window(10, TimeUnit.SECONDS)
                        .flatMap((wordCountPairObservable) -> wordCountPairObservable
                                // count how many times a word appears
                                .groupBy(WordCountPair::getWord)
                                .flatMap((groupO) -> groupO.reduce(0, (cnt, wordCntPair) -> cnt + 1)
                                        .map((cnt) -> new WordCountPair(groupO.getKey(), cnt))))
                                .map(WordCountPair::toString)
                        , StageConfigs.scalarToScalarConfig())
                // Reuse built in sink that eagerly subscribes and delivers data over SSE
                .sink(Sinks.eagerSubscribe(Sinks.sse((String data) -> data)))
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
        LocalJobExecutorNetworked.execute(new WordCountJob().getJobInstance());
    }
}

```

There are several things going on here, let's examine them one at a time...

### The Source

We specify our source in the line `.source(new IlliadSource())`. The source handles data ingestion and it is very common to use a pre-existing parameterized source when writing jobs. Mantis provides several sources which handle managing connections and queries to other jobs. In the next tutorial we'll learn how to implement our own source which ingests data from Twitter.

### The Stage

Our stage implements the bulk of the processing logic for the streaming job. Recall that a Mantis job has 1..n stages which can be used to create a topology for data processing. This stage is a [`ScalarComputation`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/computation/ScalarComputation.java) but we'll learn about other stage types in the third tutorial when we make word counting a distributed job.

We'll take advantage of Java's lambda syntax to implement this stage inline. The `call` method receives a [`Context`]() object and an `Observable<String>` provided by our source. The stage's responsibility is to produce an `Observable<R>` for consumption by down stream stages or the sink if this is the last stage.


```java
.stage((context, dataO) -> dataO
                        // Tokenize the string
                        .flatMap((text) -> Observable.from(tokenize(text)))

                        // Hopping / Tumbling window of 10 seconds
                        .window(10, TimeUnit.SECONDS)

                        // Reduce each window
                        .flatMap((wordCountPairObservable) -> wordCountPairObservable
                                // count how many times a word appears

                                .groupBy(WordCountPair::getWord)
                                .flatMap((groupO) -> groupO.reduce(0, (cnt, wordCntPair) -> cnt + 1)
                                        .map((cnt) -> new WordCountPair(groupO.getKey(), cnt))))
                                // Convert the result to a string
                                .map(WordCountPair::toString)
```

If you're familiar with reactive stream processing the above should be fairly easy to comprehend. Unfortunately if you aren't then an introduction to this is outside of the scope of this tutorial. Head over to [reactivex.io](http://reactivex.io/) to learn more about the concept.

The stage configuration below specifies a few things; First that this stage is a scalar to scalar stage in that it ingests single events, and produces single events. The type of the input events is String, and the output is also String. Finally the configuration also specifies which [`Codec`](https://github.com/Netflix/mantis/blob/master/mantis-common/src/main/java/io/mantisrx/common/codec/Codecs.java) to use on the wire for this stage's output. You can use this configuration to specify concurrency for this stage as well, but we've not elected to do so here.

```java
public static ScalarToScalar.Config<String, String> scalarToScalarConfig() {
    return new ScalarToScalar.Config<String, String>()
            .codec(Codecs.string());
}
```

### The Job Provider

The [`MantisJobProvider`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/MantisJobProvider.java) interface is what the Mantis runtime expects to load. The runtime reads [`resources/META-INF/services/io.mantisrx.runtime.MantisJobProvider`](https://github.com/Netflix/mantis-examples/blob/master/wordcount/src/main/resources/META-INF/services/io.mantisrx.runtime.MantisJobProvider) to discover the fully qualified classname of the MantisJobProvider to be used as an entry point for the application.

### Main Method

The main method invokes the [`LocalJobExecutorNetworked`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/executor/LocalJobExecutorNetworked.java) `execute` method to run our job locally. The first three tutorials will take advantage of the ability to run jobs locally. In the fourth tutorial we will explore uploading and submitting our job on a Mantis cloud deployment for greater scalability. We can and should run this main method by invoking `./gradlew :mantis-examples-wordcount:execute` at the root of the `mantis-examples` directory.

```java
    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new WordCountJob().getJobInstance());
    }

```

## Conclusions and Future Work

We've implemented a complete end-to-end Mantis job which counts words from The Illiad repeatedly. This leaves much to be desired. If you inspect our source we're really just iterating over the same data set every ten seconds. In the next tutorial we'll explore the task of writing our own custom source to pull external data from Twitter into Mantis and designing this source in a templated fashion so that it can be used with different queries and API keys.

As an extra credit task see if you can modify the stage in this job to print the top 10 words instead of the entire list.
