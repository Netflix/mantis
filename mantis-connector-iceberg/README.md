# mantis-connector-iceberg

Connector for working with [Iceberg](https://iceberg.apache.org/).

## Source

TBD

## Sink

The Iceberg Sink has two components: `Writers` and a `Committer`.

Writers write Iceberg `Record`s to files in a specific file format. Currently, supported formats
include [Parquet](https://parquet.apache.org/). File formats [Avro](https://avro.apache.org/),
[ORC](https://orc.apache.org/), and others are currently unsupported. Writers are unopinionated; you control
the semantics such as `open`, `write`, and `close`. Writers are stateless and therefore massively parallelizable.

### With a Mantis Processing Stage

The easiest way to use this sink is to use add two Mantis Processing Stages to your Mantis Job: `IcebergWriterStage`
and `IcebergCommitterStage`.

```
public class ExampleIcebergSinkJob extends MantisJobProvider<Map<String, Object>> {

    public Job<Map<String, Object>> getJobInstance() {
        return MantisJob.source(...)
                .stage(...)                                                                 // (0)
                .stage(new IcebergWriterStage(SCHEMA), IcebergWriterStage.config())         // (1)
                .stage(new IcebergCommitterStage(SCHEMA), IcebergCommitterStage.config())   // (2)
                .sink(...)
                }))
                .lifecycle(NetflixLifecycles.governator(jobPropertiesFilename,
                        new IcebergModule()))                                               // (3)
                .create();
}
```

(0) A series of your Processing Stages that you define.

(1) A Processing Stage of **n** parallelizable Iceberg Writers.

(2) A Processing Stage for **1** Iceberg Committer.

(3) A module for injecting configs required to authenticate/connect/interact with Iceberg backing infrastructure.

### Standalone

This package provides Observable Transformers for the Writer and Committer which you can compose with
an existing flow without having to instantiate separate Stages. By composing with an existing flow such as
one of your existing Processing Stages, you can avoid extra network cost from worker-to-worker communication,
trading off debuggability since your Processing Stage is interacting with Iceberg in addition to executing
your application logic.

```
IcebergWriterStage.Transformer writerTransformer =                      (0)
    new IcebergWriterStage.Transformer(writerConfig, icebergWriter);    (1)

recordObservable.compose(writerTransformer);                            (2)
```

(0) Example for Writer, but equivalent for `IcebergCommitterStage.Transformer`.

(1) Create a Transformer by passing in a WriterConfig and an IcebergWriter.

(2) Compose an upstream/existing `Observable<Record>` with the Transformer to augment with Iceberg capabilities.

### Configs

See `WriterConfig` and `CommitterConfig`.

Writers: In general, writers are parallelizable, but prefer to write larger files,
such as the default 128MB. Be careful calling `length()` in a tight loop for some file appenders, such as Parquet,
as that operation is expensive.

Committers: In general, prefer to have **1** committer per Iceberg Table. Prefer a sensible commit frequency, such as
the default 5 minutes.