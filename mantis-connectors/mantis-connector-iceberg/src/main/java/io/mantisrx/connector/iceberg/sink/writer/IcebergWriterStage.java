/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.mantisrx.connector.iceberg.sink.codecs.IcebergCodecs;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties;
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.Partitioner;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.PartitionerFactory;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.exceptions.Exceptions;
import rx.schedulers.Schedulers;

/**
 * Processing stage which writes records to Iceberg through a backing file store.
 */
public class IcebergWriterStage implements ScalarComputation<Record, DataFile> {

    private static final Logger logger = LoggerFactory.getLogger(IcebergWriterStage.class);

    private Transformer transformer;

    /**
     * Returns a config for this stage which has encoding/decoding semantics and parameter definitions.
     */
    public static ScalarToScalar.Config<Record, DataFile> config() {
        return new ScalarToScalar.Config<Record, DataFile>()
                .description("")
                .codec(IcebergCodecs.dataFile())
                .withParameters(parameters());
    }

    /**
     * Returns a list of parameter definitions for this stage.
     */
    public static List<ParameterDefinition<?>> parameters() {
        return Arrays.asList(
                new IntParameter().name(WriterProperties.WRITER_ROW_GROUP_SIZE)
                        .description(WriterProperties.WRITER_ROW_GROUP_SIZE_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(WriterProperties.WRITER_ROW_GROUP_SIZE_DEFAULT)
                        .build(),
                new StringParameter().name(WriterProperties.WRITER_FLUSH_FREQUENCY_BYTES)
                        .description(WriterProperties.WRITER_FLUSH_FREQUENCY_BYTES_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(WriterProperties.WRITER_FLUSH_FREQUENCY_BYTES_DEFAULT)
                        .build(),
                new StringParameter().name(WriterProperties.WRITER_FLUSH_FREQUENCY_MSEC)
                        .description(WriterProperties.WRITER_FLUSH_FREQUENCY_MSEC_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(WriterProperties.WRITER_FLUSH_FREQUENCY_MSEC_DEFAULT)
                        .build(),
                new StringParameter().name(WriterProperties.WRITER_FILE_FORMAT)
                        .description(WriterProperties.WRITER_FILE_FORMAT_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(WriterProperties.WRITER_FILE_FORMAT_DEFAULT)
                        .build()
        );
    }

    /**
     * Use this to instantiate a new transformer from a given {@link Context}.
     */
    public static Transformer newTransformer(Context context) {
        Configuration hadoopConfig = context.getServiceLocator().service(Configuration.class);
        WriterConfig config = new WriterConfig(context.getParameters(), hadoopConfig);
        Catalog catalog = context.getServiceLocator().service(Catalog.class);
        TableIdentifier id = TableIdentifier.of(config.getCatalog(), config.getDatabase(), config.getTable());
        Table table = catalog.loadTable(id);
        WorkerInfo workerInfo = context.getWorkerInfo();

        LocationProvider locationProvider = context.getServiceLocator().service(LocationProvider.class);
        IcebergWriter writer = new DefaultIcebergWriter(config, workerInfo, table, locationProvider);
        WriterMetrics metrics = new WriterMetrics();
        PartitionerFactory partitionerFactory = context.getServiceLocator().service(PartitionerFactory.class);
        Partitioner partitioner = partitionerFactory.getPartitioner(table);

        return new Transformer(config, metrics, writer, partitioner, Schedulers.computation(), Schedulers.io());
    }

    public IcebergWriterStage() {
    }

    /**
     * Uses the provided Mantis Context to inject configuration and opens an underlying file appender.
     * <p>
     * This method depends on a Hadoop Configuration and Iceberg Catalog, both injected
     * from the Context's service locator.
     * <p>
     * Note that this method expects an Iceberg Table to have been previously created out-of-band,
     * otherwise initialization will fail. Users should prefer to create tables
     * out-of-band so they can be versioned alongside their schemas.
     */
    @Override
    public void init(Context context) {
        transformer = newTransformer(context);
    }

    @Override
    public Observable<DataFile> call(Context context, Observable<Record> recordObservable) {
        return recordObservable.compose(transformer);
    }

    /**
     * Reactive Transformer for writing records to Iceberg.
     * <p>
     * Users may use this class independently of this Stage, for example, if they want to
     * {@link Observable#compose(Observable.Transformer)} this transformer with a flow into
     * an existing Stage. One benefit of this co-location is to avoid extra network
     * cost from worker-to-worker communication, trading off debuggability.
     */
    public static class Transformer implements Observable.Transformer<Record, DataFile> {

        private static final DataFile ERROR_DATA_FILE = new DataFiles.Builder()
                .withPath("/error.parquet")
                .withFileSizeInBytes(0L)
                .withRecordCount(0L)
                .build();

        private static final Schema TIMER_SCHEMA = new Schema(
                Types.NestedField.required(1, "ts_utc_msec", Types.LongType.get()));

        private static final Record TIMER_RECORD = GenericRecord.create(TIMER_SCHEMA);

        private final WriterConfig config;
        private final WriterMetrics metrics;
        private final Partitioner partitioner;
        private final IcebergWriter writer;
        private final Scheduler timerScheduler;
        private final Scheduler transformerScheduler;

        public Transformer(
                WriterConfig config,
                WriterMetrics metrics,
                IcebergWriter writer,
                Partitioner partitioner,
                Scheduler timerScheduler,
                Scheduler transformerScheduler) {
            this.config = config;
            this.metrics = metrics;
            this.writer = writer;
            this.partitioner = partitioner;
            this.timerScheduler = timerScheduler;
            this.transformerScheduler = transformerScheduler;
        }

        /**
         * Opens an IcebergWriter FileAppender, writes records to a file. The appender flushes if any of
         * following criteria is met, in order of precedence:
         * <p>
         * 1. new partition
         * 2. size threshold
         * 3. time threshold
         * <p>
         * New Partition:
         * <p>
         * If the Iceberg Table is partitioned, the appender will check _every_ record to detect a new partition.
         * If there's a new partition, the appender will align the record to the new partition. It does this by
         * closing the current file, opening a new file, and writing the record to that new file.
         * <p>
         * It's _important_ that upstream producers align events to partitions as best as possible. For example,
         * - Given an Iceberg Table partitioned by {@code hour}
         * - 10 producers writing Iceberg Records
         * <p>
         * Each of the 10 producers _should_ try to produce events aligned by the hour.
         * If writes are not well-aligned, then results will be correct, but performance negatively impacted due to
         * frequent opening/closing of files.
         * <p>
         * Writes may be _unordered_ as long as they're aligned by the table's partitioning.
         * <p>
         * Size Threshold:
         * <p>
         * The appender will periodically check the current file size as configured by
         * {@link WriterConfig#getWriterRowGroupSize()}. If it's time to check, then the appender will flush on
         * {@link WriterConfig#getWriterFlushFrequencyBytes()}.
         * <p>
         * Time Threshold:
         * <p>
         * The appender will periodically attempt to flush as configured by
         * {@link WriterConfig#getWriterFlushFrequencyMsec()}. If this threshold is met, the appender will flush
         * only if the appender has an open file. This avoids flushing unnecessarily if there are no events.
         * Otherwise, a flush will happen, even if there are few events in the file. This effectively limits the
         * upper-bound for allowed lateness.
         * <p>
         * Pair this writer with a progressive multipart file uploader backend for better latencies.
         */
        @Override
        public Observable<DataFile> call(Observable<Record> source) {
            Observable<Record> timer = Observable.interval(
                    config.getWriterFlushFrequencyMsec(), TimeUnit.MILLISECONDS, timerScheduler)
                    .map(i -> TIMER_RECORD);

            return source.mergeWith(timer)
                    .observeOn(transformerScheduler)
                    .scan(new Trigger(config.getWriterRowGroupSize()), (trigger, record) -> {
                        if (record.struct().fields().equals(TIMER_SCHEMA.columns())) {
                            trigger.timeout();
                        } else {
                            StructLike partition = partitioner.partition(record);
                            // Only open (if closed) on new events from `source`; exclude timer records.
                            if (writer.isClosed()) {
                                try {
                                    logger.info("opening file for partition {}", partition);
                                    writer.open(partition);
                                    trigger.setPartition(partition);
                                    metrics.increment(WriterMetrics.OPEN_SUCCESS_COUNT);
                                } catch (IOException e) {
                                    metrics.increment(WriterMetrics.OPEN_FAILURE_COUNT);
                                    throw Exceptions.propagate(e);
                                }
                            }

                            // Make sure records are aligned with the partition.
                            if (trigger.isNewPartition(partition)) {
                                trigger.setPartition(partition);

                                try {
                                    DataFile dataFile = writer.close();
                                    trigger.stage(dataFile);
                                    trigger.reset();
                                } catch (IOException | RuntimeException e) {
                                    metrics.increment(WriterMetrics.BATCH_FAILURE_COUNT);
                                    logger.error("error writing DataFile", e);
                                }

                                try {
                                    logger.info("opening file for new partition {}", partition);
                                    writer.open(partition);
                                    metrics.increment(WriterMetrics.OPEN_SUCCESS_COUNT);
                                } catch (IOException e) {
                                    metrics.increment(WriterMetrics.OPEN_FAILURE_COUNT);
                                    throw Exceptions.propagate(e);
                                }
                            }

                            try {
                                writer.write(record);
                                trigger.increment();
                                metrics.increment(WriterMetrics.WRITE_SUCCESS_COUNT);
                            } catch (RuntimeException e) {
                                metrics.increment(WriterMetrics.WRITE_FAILURE_COUNT);
                                logger.debug("error writing record {}", record);
                            }
                        }

                        return trigger;
                    })
                    .filter(this::shouldFlush)
                    // Writer can be closed if there are no events, yet timer is still ticking.
                    .filter(trigger -> !writer.isClosed())
                    .map(trigger -> {
                        // Triggered by new partition.
                        if (trigger.hasStagedDataFile()) {
                            DataFile dataFile = trigger.getStagedDataFile().copy();
                            trigger.clearStagedDataFile();
                            return dataFile;
                        }

                        // Triggered by size or time.
                        try {
                            DataFile dataFile = writer.close();
                            trigger.reset();
                            return dataFile;
                        } catch (IOException | RuntimeException e) {
                            metrics.increment(WriterMetrics.BATCH_FAILURE_COUNT);
                            logger.error("error writing DataFile", e);
                            return ERROR_DATA_FILE;
                        }
                    })
                    .filter(dataFile -> !isErrorDataFile(dataFile))
                    .doOnNext(dataFile -> {
                        metrics.increment(WriterMetrics.BATCH_SUCCESS_COUNT);
                        logger.info("writing DataFile: {}", dataFile);
                        metrics.setGauge(WriterMetrics.BATCH_SIZE, dataFile.recordCount());
                        metrics.setGauge(WriterMetrics.BATCH_SIZE_BYTES, dataFile.fileSizeInBytes());
                    })
                    .doOnTerminate(() -> {
                        try {
                            logger.info("closing writer on rx terminate signal");
                            writer.close();
                        } catch (IOException e) {
                            throw Exceptions.propagate(e);
                        }
                    })
                    .share();
        }

        private boolean isErrorDataFile(DataFile dataFile) {
            return Comparators.charSequences().compare(ERROR_DATA_FILE.path(), dataFile.path()) == 0 &&
                    ERROR_DATA_FILE.fileSizeInBytes() == dataFile.fileSizeInBytes() &&
                    ERROR_DATA_FILE.recordCount() == dataFile.recordCount();
        }

        /**
         * Trigger a flush on a repartition event, size threshold, or time threshold.
         */
        private boolean shouldFlush(Trigger trigger) {
            // For size threshold, check the trigger to short-circuit if the count is over the threshold first
            // because implementations of `writer.length()` may be expensive if called in a tight loop.
            return trigger.hasStagedDataFile()
                    || (trigger.isOverCountThreshold() && writer.length() >= config.getWriterFlushFrequencyBytes())
                    || trigger.isTimedOut();
        }

        private static class Trigger {

            private final int countThreshold;

            private int counter;
            private boolean timedOut;
            private StructLike partition;
            private DataFile stagedDataFile;

            Trigger(int countThreshold) {
                this.countThreshold = countThreshold;
            }

            void increment() {
                counter++;
            }

            void timeout() {
                timedOut = true;
            }

            void setPartition(StructLike newPartition) {
                partition = newPartition;
            }

            void reset() {
                counter = 0;
                timedOut = false;
            }

            void stage(DataFile dataFile) {
                this.stagedDataFile = dataFile;
            }

            boolean isOverCountThreshold() {
                return counter >= countThreshold;
            }

            boolean isTimedOut() {
                return timedOut;
            }

            boolean isNewPartition(StructLike newPartition) {
                return partition != null && !partition.equals(newPartition);
            }

            boolean hasStagedDataFile() {
                return stagedDataFile != null;
            }

            DataFile getStagedDataFile() {
                return stagedDataFile;
            }

            void clearStagedDataFile() {
                stagedDataFile = null;
            }

            @Override
            public String toString() {
                return "Trigger{"
                        + " countThreshold=" + countThreshold
                        + ", counter=" + counter
                        + ", timedOut=" + timedOut
                        + ", partition=" + partition
                        + ", stagedDataFile=" + stagedDataFile
                        + '}';
            }
        }
    }
}
