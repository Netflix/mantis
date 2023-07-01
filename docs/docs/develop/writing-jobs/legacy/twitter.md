# [Legacy, RxJava] Mantis Job - Writing Custom Source

!!! note

    There is an alternate implementation that allows writing a Mantis Job as a series of operators operating directly on a `MantisStream` instance which abstracts information about RxJava, Observables from the user and offers a simpler way (hopefully 🤞) to write mantis jobs. Please see [Mantis DSL docs](../../../../reference/dsl) for more details or [this documentation](../../twitter) for the sample job.

Our first tutorial primed us for writing and executing a job end-to-end but it wasn't particularly interesting from a data perspective because it just repeatedly looped over the contents of a book. In this example we'll explore writing a more involved source which reads an infinite stream of data from Twitter and performs the same word count in real-time. Mantis jobs can easily subscribe to one another using some built in sources but the technique in this tutorial can be used to pull external data into the Mantis ecosystem.

To proceed you'll need to head over to Twitter and grab yourself a pair of API keys.

## The Source
The source is responsible for ingesting data to be processed within the job. Many Mantis jobs will subscribe to other jobs and can simply use a templatized source such as [`io.mantisrx.connectors.job.source.JobSource`](https://github.com/Netflix/mantis/blob/master/mantis-connectors/mantis-connector-job/src/main/java/io/mantisrx/connector/job/source/JobSource.java) which handles all the minutiae of connecting to other jobs for us. If however your job exists on the edge of Mantis it will need to pull data in via a custom source. Since we're reading from the Twitter API we'll need to do this ourselves.

Our [`TwitterSource`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/source/Source.java) must implement [`io.mantisrx.runtime.source.Source`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/source/Source.java) which requires us to implement `call` and optionally `init`. Mantis provides some guarantees here in that `init` will be invoked exactly once and before `call` which will be invoked at least once. This makes `init` the ideal location to perform one time setup and configuration for the source and `call` the ideal location for performing work on the incoming stream. The objective of this entire class is to have `call` return an `Observable<Observable<T>>` which will be passed as a parameter to the first stage of our job.

Let's deconstruct the `init` method first. Here we will extract our parameters from the `Context` -- this allows us to write more generic sources which can be templatized and reused across many jobs. This is a very common pattern for writing Mantis jobs and allows you to iterate quickly testing various configurations as jobs can be resubmitted easily with new parameters.

```java
/**
  * Init method is called only once during initialization. It is the ideal place to perform one time
  * configuration actions.
  *
  * @param context Provides access to Mantis system information like JobId, Job parameters etc
  * @param index   This provides access to the unique workerIndex assigned to this container. It also provides
  *                the total number of workers of this job.
  */
@Override
public void init(Context context, Index index) {

    String consumerKey = (String) context.getParameters().get(CONSUMER_KEY_PARAM);
    String consumerSecret = (String) context.getParameters().get(CONSUMER_SECRET_PARAM);
    String token = (String) context.getParameters().get(TOKEN_PARAM);
    String tokenSecret = (String) context.getParameters().get(TOKEN_SECRET_PARAM);
    String terms = (String) context.getParameters().get(TERMS_PARAM);

    Authentication auth = new OAuth1(consumerKey,
            consumerSecret,
            token,
            tokenSecret);

    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

    String[] termArray = terms.split(",");

    List<String> termsList = Arrays.asList(termArray);

    endpoint.trackTerms(termsList);

    client = new ClientBuilder()
            .name("twitter-source")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(twitterObservable))
            .build();


    client.connect();
}
```

Our `call` method is very simple thanks to the fact that our twitter client writes to a custom `BlockingQueue` adapter that we've written. We simply need to return an `Observable<Observable<T>>`.

```java
@Override
public Observable<Observable<String>> call(Context context, Index index) {
    return Observable.just(twitterObservable.observe());
}

```

You may have noticed that our `init` method is pulling a bunch of parameters out of the [`Context`](https://github.com/Netflix/mantis/blob/master/mantis-runtime/src/main/java/io/mantisrx/runtime/Context.java). These are specified in `Source#getParameters()` and allow us to parameterize this source so that different instances of this job may work with different parameters. This is a very useful concept for designing reusable components for constructing jobs as well as completely reusable jobs.

```java
    /**
     * Define parameters required by this source.
     *
     * @return
     */
    @Override
    public List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = Lists.newArrayList();

        // Consumer key
        params.add(new StringParameter()
                .name(CONSUMER_KEY_PARAM)
                .description("twitter consumer key")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(CONSUMER_SECRET_PARAM)
                .description("twitter consumer secret")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(TOKEN_PARAM)
                .description("twitter token")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(TOKEN_SECRET_PARAM)
                .description("twitter token secret")
                .validator(Validators.notNullOrEmpty())
                .required()
                .build());

        params.add(new StringParameter()
                .name(TERMS_PARAM)
                .description("terms to follow")
                .validator(Validators.notNullOrEmpty())
                .defaultValue("Netflix,Dark")
                .build());

        return params;

    }
```

Now our primary class `TwitterJob` which implements `MantisJobProvider` needs to specify our new source so change the source line to match the following.

```java
.source(new TwitterSource())
```

## The Stage

The stage is nearly equivalent to the previous tutorial. We need to add a few lines to the beginning of the chain of operations to deserialize the string, filter for English Tweets and pluck out the text.


```java
            .stage((context, dataO) -> dataO

                    // Deserialize data
                    .map(JsonUtility::jsonToMap)

                    // Filter for English Tweets
                    .filter((eventMap) -> {
                        if(eventMap.containsKey("lang") && eventMap.containsKey("text")) {
                            String lang = (String)eventMap.get("lang");
                            return "en".equalsIgnoreCase(lang);
                        }
                        return false;
                    })

                    // Extract Tweet body
                    .map((eventMap) -> (String)eventMap.get("text"))

                    // Same from here...
```

# Conclusion
We've learned how to create a parameterized source which reads from Twitter and pulls data into the ecosystem. With some slight modifications our previous example's stage deserializes the messages and extracts the data to perform the same word count.

If you've checked out the [`mantis`](https://github.com/Netflix/mantis) repository, then running following commands should begin running the job and expose a local port for SSE streaming.
```bash
$ cd mantis-examples/mantis-examples-twitter-sample
$ ../../gradlew execute --args='consumerKey consumerSecret token tokensecret'
```
As an exercise consider how you might begin to scale this work out over multiple machines if the workload were too large to perform on a single host. This will be the topic of the next tutorial.
