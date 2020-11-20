## Sink

Add this package to your dependencies:

--8<-- "snippets/develop/connectors/open-source-iceberg-dependency.md"

The Iceberg Sink has two components: `Writers` and a `Committer`.

### Writers

**Writers** write Iceberg `Record`s to files in a specific file format. Writers periodically
stage their data by flushing their underlying files to produce metadata in the form of
`DataFile`s which are sent to Committers.

Add an Iceberg Writer using one of the following approaches:

#### Separate Processing Stage

Use this approach to decouple your application logic from Iceberg writing logic to
may make it easier to debug your Mantis Job. This approach incurs extra encode/decode and
network cost to move data between workers.

```java hl_lines="6 7"
public class ExampleIcebergSinkJob extends MantisJobProvider<Map<String, Object>> {

  @Override
  public Job<Map<String, Object>> getJobInstance() {
      return MantisJob.source(<source that produces MantisServerSentEvents>)
          .stage(...)                                                           // (0)
          .stage(new IcebergWriterStage(), IcebergWriterStage.config())         // (1)
          .stage(new IcebergCommitterStage(), IcebergCommitterStage.config())   // (2)
          .sink(...)
          .lifecycle(NetflixLifecycles.governator(
              <job file properties name>,
              new IcebergModule()))                                             // (3)
          .create();
  }
}
```

> **(0)** A series of Processing Stages that you define for your application logic. Produces an Iceberg Record.
> To emit Iceberg Records out of your separate Processing Stage, add the [Iceberg Record Codec](https://github.com/Netflix/mantis/blob/master/mantis-connectors/mantis-connector-iceberg/src/main/java/io/mantisrx/connector/iceberg/sink/codecs/IcebergCodecs.java#L42) to your
> stage config.

> **(1)** A Processing Stage of **n** parallelizable `IcebergWriter`s. The Stage Config automatically adds
> an [Iceberg DataFile Codec](https://github.com/Netflix/mantis/blob/master/mantis-connectors/mantis-connector-iceberg/src/main/java/io/mantisrx/connector/iceberg/sink/codecs/IcebergCodecs.java#L49) to encode/decode DataFiles between these workers and the Committer
> workers from the next stage downstream.

> **(2)** A Processing Stage for **1** `IcebergCommitter`.

> **(3)** A module for injecting dependencies and configs required to authenticate/connect/interact
> with backing Iceberg infrastructure.

#### With an existing Processing Stage

Use this approach to avoid incurring encode/decode and network costs. This approach may
make your Mantis Job more difficult to debug.

```java hl_lines="6 23 34 39 40 41 42"
public class ExampleIcebergSinkJob extends MantisJobProvider<Map<String, Object>> {

  @Override
  public Job<Map<String, Object>> getJobInstance() {
      return MantisJob.source(<source that produces MantisServerSentEvents>)
          .stage(new ProcessingAndWriterStage(), <config>)                      // (0)
          .stage(new IcebergCommitterStage(), IcebergCommitterStage.config())   // (1)
          .sink(...)
          .lifecycle(NetflixLifecycles.governator(
              <job file properties name>,
              new IcebergModule()))                                             // (2)
          .create();
  }
}

/**
 * Example class which takes in MantisServerSentEvents, performs some logic,
 * produces Iceberg Records, writes the Records to files,
 * and produces DataFiles for a downstream Iceberg Committer.
 */
public class ProcessingAndWriterStage implements ScalarComputation<MantisServerSentEvent, DataFile> {

  private Transformer transformer;

  public static ScalarToScalar.Config<Record, DataFile> config() {
    return new ScalarToScalar.Config<Record, DataFile>()
        .description("")
        .codec(IcebergCodecs.dataFile())                                        // (3)
        .withParameters(...);
  }

  @Override
  public void init(Context context) {
    transformer = IcebergWriterStage.newTransformer(context);                   // (4)
  }

  @Override
  public Observable<DataFile> call(Context context, Observable<Record> recordObservable) {
    return recordObservable
        .map(<some application logic>)
        .map(<some more application logic>)
        .compose(transformer);                                                  // (5)
  }
}
```

> **(0)** A series of Processing Stages for your application logic _and_ Iceberg writing logic.
> You may further reduce network cost by combining your Processing Stage(s) logic into your Source.

> **(1)** A Processing Stage for **1** `IcebergCommitter`.

> **(2)** A module for injecting dependencies and configs required to authenticate/connect/interact
> with backing Iceberg infrastructure.

> **(3)** Remember to add an Iceberg DataFile Codec to emit `DataFile`s to the Committer.

> **(4)** Create a new Iceberg Writer [Transformer](http://reactivex.io/RxJava/javadoc/io/reactivex/ObservableTransformer.html)
> from the static method provided by `IcebergWriterStage`.

> **(5)** [Compose](http://reactivex.io/RxJava/javadoc/io/reactivex/Observable.html#compose-io.reactivex.ObservableTransformer-)
> the transformer with your application logic Observable.

!!! note
    Writers are stateless and may be parallelized/autoscaled.

### Committer

The **Committer** commits `DataFile`s to Iceberg. `Records` are queryable from the Iceberg table
only after a Committer commits `DataFile`s. A Committer commits on a configured interval (default: 5 min).

If a Committer fails, it will retry until a retry threshold is met, after which it will continue
onto the next window of `Record`s. This avoids backpressure issues originating from downstream consumers
which makes it more suitable for operational use cases.

```java hl_lines="8"
public class ExampleIcebergSinkJob extends MantisJobProvider<Map<String, Object>> {

  @Override
  public Job<Map<String, Object>> getJobInstance() {
      return MantisJob.source(<source that produces Iceberg Records>)
          .stage(...)
          .stage(new IcebergWriterStage(), IcebergWriterStage.config())
          .stage(new IcebergCommitterStage(), IcebergCommitterStage.config())   // (1)
          .sink(...)
          .lifecycle(NetflixLifecycles.governator(
              <job file properties name>,
              new IcebergModule()))
          .create();
  }
}
```

> **(1)** A Processing Stage for **1** `IcebergCommitter`. The Committer outputs a `Map` which
> you can subscribe to with a Sink for optional debugging or connecting to another Mantis Job.
> Otherwise, a Sink not required because the Committer will write directly to Iceberg.

!!! important
    You should try to have only **one** Committer per Iceberg Table and try to avoid a high frequency commit
    intervals (default: `5 min`). This avoids commit pressure on Iceberg.

### Configuration Options

| **Name** | **Type** | **Default** | **Description** |
| -------- | -------- | ----------- | --------------- |
| `writerRowGroupSize` | int | 100 | Number of rows to chunk before checking for file size |
| `writerFlushFrequencyBytes` | String | "134217728" (128 MiB) | Flush frequency by size in Bytes |
| `writerFlushFrequencyMsec` | String | "60000" (1 min) | Flush frequency by time in milliseconds |
| `writerFileFormat` | String | parquet | File format for writing data files to backing Iceberg store |
| `writerMaximumPoolSize` | int | 5 | Maximum number of writers that should exist per worker |
| `commitFrequencyMs` | String | "300000" (5 min) | Iceberg Committer frequency by time in milliseconds |

### Metrics

Prefix: `io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics`

| **Name** | **Type** | **Description** |
| -------- | -------- | --------------- |
| `openSuccessCount` | Counter | Number of times a file was successfully opened |
| `openFailureCount` | Counter | Number of times a file failed to open |
| `writeSuccessCount` | Counter | Number of times an Iceberg `Record` was successfully written to a file |
| `writeFailureCount` | Counter | Number of times an Iceberg `Record` failed to be written to a file |
| `batchSuccessCount` | Counter | Number of times a file was successfully flushed to produce a `DataFile` |
| `batchFailureCount` | Counter | Number of times a file failed to flush |
| `batchSize` | Gauge | Number of Iceberg `Records` per writer flush as described within a `DataFile` |
| `batchSizeBytes` | Gauge | Cumulative size of Iceberg `Records` from a writer flush |

Prefix: `io.mantisrx.connector.iceberg.sink.committer.metrics.CommitterMetrics`

| **Name** | **Type** | **Description** |
| -------- | -------- | --------------- |
| `invocationCount` | Counter | Number of times a commit was invoked |
| `commitSuccessCount` | Counter | Number of times a commit was successful |
| `commitFailureCount` | Counter | Number of times a commit failed |
| `commitLatencyMsec` | Gauge | Time it took to perform the most recent commit |
| `commitBatchSize` | Gauge | Cumulative size of `DataFile`s from a commit |
