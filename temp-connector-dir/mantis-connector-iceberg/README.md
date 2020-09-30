# mantis-connector-iceberg

Connector for working with [Iceberg](https://iceberg.apache.org/).

## Sink

Add this package to your dependencies:

```groovy
implementation "io.mantisrx:mantis-connector-iceberg:$icebergVersion"
```

The Iceberg Sink has two components: `Writers` and a `Committer`.

### Writers

**Writers** write Iceberg `Record`s to files in a specific file format. Writers periodically
stage their data by flushing their underlying files to produce metadata in the form of
`DataFile`s which are sent to Committers.

You can instantiate and use an `IcebergWriter` in a separate Processing Stage or with
an existing Processing Stage.

#### Separate Processing Stage

Use this approach to decouple your application logic from Iceberg writing logic to
may make it easier to debug your Mantis Job. This approach incurs extra encode/decode and
network cost to move data between workers.

```java
public class ExampleIcebergSinkJob extends MantisJobProvider<Map<String, Object>> {

  @Override
  public Job<Map<String, Object>> getJobInstance() {
      return MantisJob.source(<source that produces Iceberg Records>)
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

> **(0)** A series of Processing Stages that you define for your application logic.

> **(1)** A Processing Stage of **n** parallelizable `IcebergWriter`s. The Stage Config automatically adds
> an [Iceberg DataFile Codec](https://github.com/Netflix/mantis-connectors/blob/master/mantis-connector-iceberg/src/main/java/io/mantisrx/connector/iceberg/sink/codecs/IcebergCodecs.java#L49) to encode/decode DataFiles between these workers and the Committer
> workers from the next stage downstream.

> **(2)** A Processing Stage for **1** `IcebergCommitter`.

> **(3)** A module for injecting dependencies and configs required to authenticate/connect/interact
> with backing Iceberg infrastructure.

```
note
    To emit Iceberg Records out of your separate Processing Stage, use the [Iceberg Record Codec](https://github.com/Netflix/mantis-connectors/blob/master/mantis-connector-iceberg/src/main/java/io/mantisrx/connector/iceberg/sink/codecs/IcebergCodecs.java#L42) to encode/decode
    Iceberg Records between workers.
```

#### With an existing Processing Stage

Use this approach to avoid incurring encode/decode and network costs. This approach may
make your Mantis Job more difficult to debug.

```java
public class ExampleIcebergSinkJob extends MantisJobProvider<Map<String, Object>> {

  @Override
  public Job<Map<String, Object>> getJobInstance() {
      return MantisJob.source(<source that produces Iceberg Records>)
          .stage(new ColocatedRecordProcessingStage(), <config>)                // (0)
          .stage(new IcebergCommitterStage(), IcebergCommitterStage.config())   // (1)
          .sink(...)
          .lifecycle(NetflixLifecycles.governator(
              <job file properties name>,
              new IcebergModule()))                                             // (2)
          .create();
  }
}

/**
 * Example class which takes in Iceberg Records, performs some logic, writes
 * the Records to files, and produces DataFiles for a downstream Iceberg Committer.
 */
public class ColocatedRecordProcessingStage implements ScalarComputation<Record, DataFile> {

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

> **(3)** Remember to add an Iceberg DataFile Codec to emit `DataFile`s` to the Committer.

> **(4)** Create a new Iceberg Writer [Transformer](http://reactivex.io/RxJava/javadoc/io/reactivex/ObservableTransformer.html)
> from the static method provided by `IcebergWriterStage`.

> **(5)** [Compose](http://reactivex.io/RxJava/javadoc/io/reactivex/Observable.html#compose-io.reactivex.ObservableTransformer-)
> the transformer with your application logic Observable.

```
note
    Writers are stateless and may be parallelized/autoscaled.
```

```
important
    To avoid poor write performance for _partitioned Iceberg Tables_, make sure your upstream producers
    write Iceberg Records in alignment with the table's partitioning as best they can. 

    For example:

    - Given an Iceberg Table partitioned by `hour`
    - 10 upstream producers writing Iceberg Records

    Each of the 10 producers _should_ try to produce events aligned by the hour.

    Writes may be _unordered_; the only concern is aligning writes to the table's partitioning.

    If writes are not well-aligned, then results will be correct, but write performance negatively impacted.
```

### Committer

```java
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

A **Committer** commits `DataFile`s to Iceberg. `Records` are queryable from the Iceberg table
only after a Committer commits `DataFile`s. A Committer commits on a configured interval (default: 5 min).

A Committer aligns to Mantis's _at-most-once_ guarantee. This means if a commit fails, it
_will not_ retry that commit. It will instead continue and try to commit for the next window
of `Record`s at the next interval. This avoids backpressure issues originating from downstream
and is therefore more suitable to operational use cases.

```
important
    You should have only **one** Committer per Iceberg Table and try to avoid a high frequency commit
    intervals (default: `5 min`). This avoids commit pressure on Iceberg.
```

### Configuration Options

| **Name** | **Type** | **Default** | **Description** |
| -------- | -------- | ----------- | --------------- |
| `writerRowGroupSize` | int | 100 | Number of rows to chunk before checking for file size |
| `writerFlushFrequencyBytes` | String | "134217728" (128 MiB) | Flush frequency by size in Bytes |
| `writerFlushFrequencyMsec` | String | "60000" (1 min) | Flush frequency by time in milliseconds |
| `writerFileFormat` | String | parquet | File format for writing data files to backing Iceberg store |
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