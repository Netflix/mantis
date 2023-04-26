/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink.writer;

import io.mantisrx.connector.iceberg.sink.codecs.IcebergCodecs;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties;
import io.mantisrx.connector.iceberg.sink.writer.factory.DefaultIcebergWriterFactory;
import io.mantisrx.connector.iceberg.sink.writer.factory.IcebergWriterFactory;
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.Partitioner;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.PartitionerFactory;
import io.mantisrx.connector.iceberg.sink.writer.pool.FixedIcebergWriterPool;
import io.mantisrx.connector.iceberg.sink.writer.pool.IcebergWriterPool;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.scheduler.MantisRxSingleThreadScheduler;
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.LocationProvider;
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
public class IcebergWriterStage implements ScalarComputation<MantisRecord, MantisDataFile> {

    private static final Logger logger = LoggerFactory.getLogger(IcebergWriterStage.class);

    private Transformer transformer;

    /**
     * Returns a config for this stage which has encoding/decoding semantics and parameter definitions.
     */
    public static ScalarToScalar.Config<MantisRecord, MantisDataFile> config() {
        return new ScalarToScalar.Config<MantisRecord, MantisDataFile>()
                .description("")
                .codec(IcebergCodecs.mantisDataFile())
                .serialInput()
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
                        .build(),
                new IntParameter().name(WriterProperties.WRITER_MAXIMUM_POOL_SIZE)
                    .description(WriterProperties.WRITER_MAXIMUM_POOL_SIZE_DESCRIPTION)
                    .validator(Validators.alwaysPass())
                    .defaultValue(WriterProperties.WRITER_MAXIMUM_POOL_SIZE_DEFAULT)
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
        IcebergWriterFactory factory = new DefaultIcebergWriterFactory(config, workerInfo, table, locationProvider);
        IcebergWriterPool writerPool = new FixedIcebergWriterPool(factory, config);
        WriterMetrics metrics = new WriterMetrics();
        PartitionerFactory partitionerFactory = context.getServiceLocator().service(PartitionerFactory.class);
        Partitioner partitioner = partitionerFactory.getPartitioner(table);

        return newTransformer(config, metrics, writerPool, partitioner, context.getWorkerInfo(), context.getClassLoader());
    }

    @VisibleForTesting
    static Transformer newTransformer(
            WriterConfig writerConfig,
            WriterMetrics writerMetrics,
            IcebergWriterPool writerPool,
            Partitioner partitioner,
            WorkerInfo workerInfo,
            @Nullable ClassLoader loader) {
        int workerIdx = workerInfo.getWorkerIndex();
        String nameFormat = "IcebergWriter (" + (workerIdx + 1) + ")-%d";
        ThreadFactoryBuilder tfBuilder = new ThreadFactoryBuilder().setNameFormat(nameFormat);
        if(loader != null) {
            // if the job classloader is enabled, let's use the job classloader
            // as the thread context classloader of these threads
            ThreadFactory backingTf = r -> {
                Thread thread = new Thread(r);
                thread.setContextClassLoader(loader);
                return thread;
            };
            tfBuilder.setThreadFactory(backingTf);
        }
        Scheduler executingService = new MantisRxSingleThreadScheduler(tfBuilder.build());
        return new Transformer(writerConfig, writerMetrics, writerPool, partitioner,
                Schedulers.computation(), executingService);
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
    public Observable<MantisDataFile> call(Context context, Observable<MantisRecord> recordObservable) {
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
    public static class Transformer implements Observable.Transformer<MantisRecord, MantisDataFile> {

        private static final Schema TIMEOUT_SCHEMA = new Schema(
            Types.NestedField.required(1, "ts_utc_msec", Types.LongType.get()));

        private static final MantisRecord TIMEOUT_RECORD =
            new MantisRecord(GenericRecord.create(TIMEOUT_SCHEMA), null);

        private final WriterConfig config;
        private final WriterMetrics metrics;
        private final Partitioner partitioner;
        private final IcebergWriterPool writerPool;
        private final Scheduler timerScheduler;
        private final Scheduler transformerScheduler;

        public Transformer(
            WriterConfig config,
            WriterMetrics metrics,
            IcebergWriterPool writerPool,
            Partitioner partitioner,
            Scheduler timerScheduler,
            Scheduler transformerScheduler) {
            this.config = config;
            this.metrics = metrics;
            this.writerPool = writerPool;
            this.partitioner = partitioner;
            this.timerScheduler = timerScheduler;
            this.transformerScheduler = transformerScheduler;
        }

        /**
         * Opens an IcebergWriter FileAppender, writes records to a file. The appender flushes on size or time
         * threshold.
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
        public Observable<MantisDataFile> call(Observable<MantisRecord> source) {
            Observable<MantisRecord> timer = Observable.interval(
                    config.getWriterFlushFrequencyMsec(), TimeUnit.MILLISECONDS, timerScheduler)
                .map(i -> TIMEOUT_RECORD);

            return source.mergeWith(timer)
                .observeOn(transformerScheduler)
                .scan(new Trigger(config.getWriterRowGroupSize()), (trigger, record) -> {
                    if (record.getRecord().struct().fields().equals(TIMEOUT_SCHEMA.columns())) {
                        // Timeout; track all writers even if they're not yet ready to be flushed.
                        trigger.trackAll(writerPool.getWriters());
                    } else {
                        StructLike partition = partitioner.partition(record.getRecord());

                        if (writerPool.isClosed(partition)) {
                            try {
                                logger.info("opening file for partition {}", partition);
                                writerPool.open(partition);
                                metrics.increment(WriterMetrics.OPEN_SUCCESS_COUNT);
                            } catch (IOException e) {
                                metrics.increment(WriterMetrics.OPEN_FAILURE_COUNT);
                                throw Exceptions.propagate(e);
                            }
                        }

                        try {
                            writerPool.write(partition, record);
                            trigger.increment();
                            // Check all writers to see if any are flushable and track them if so.
                            // We should check _all_ writers because a writer that should be flushable
                            // may not be flushed if an event for its partition doesn't show up for
                            // a period of time. This could cause a longer delay for that partition,
                            // especially since we only check at the trigger's count threshold.
                            if (trigger.isOverCountThreshold()) {
                                trigger.trackAll(writerPool.getFlushableWriters());
                            }
                            metrics.increment(WriterMetrics.WRITE_SUCCESS_COUNT);
                        } catch (RuntimeException e) {
                            metrics.increment(WriterMetrics.WRITE_FAILURE_COUNT);
                            logger.debug("error writing record {}", record);
                        }
                    }

                    return trigger;
                })
                .filter(Trigger::shouldFlush)
                .map(trigger -> {
                    List<MantisDataFile> dataFiles = new ArrayList<>();

                    // Timer can still tick while no writers are open (i.e., no events), which means
                    // there won't be any tracked writers.
                    for (StructLike partition : trigger.getTrackedWriters()) {
                        try {
                            MantisDataFile dataFile = writerPool.close(partition);
                            dataFiles.add(dataFile);
                        } catch (IOException | RuntimeException e) {
                            metrics.increment(WriterMetrics.BATCH_FAILURE_COUNT);
                            logger.error("error writing DataFile", e);
                        }
                    }
                    trigger.reset();

                    return dataFiles;
                })
                .filter(dataFiles -> !dataFiles.isEmpty())
                .flatMapIterable(t -> t)
                .doOnNext(dataFile -> {
                    metrics.increment(WriterMetrics.BATCH_SUCCESS_COUNT);
                    logger.info("writing DataFile: {}", dataFile);
                    metrics.setGauge(WriterMetrics.BATCH_SIZE, dataFile.getDataFile().recordCount());
                    metrics.setGauge(WriterMetrics.BATCH_SIZE_BYTES, dataFile.getDataFile().fileSizeInBytes());
                })
                .doOnTerminate(() -> {
                    try {
                        logger.info("closing writer on rx terminate signal");
                        writerPool.closeAll();
                    } catch (IOException e) {
                        throw Exceptions.propagate(e);
                    }
                })
                .share();
        }

        private static class Trigger {

            private final int countThreshold;

            private final Set<StructLike> writers;

            private int counter;

            Trigger(int countThreshold) {
                this.countThreshold = countThreshold;
                writers = new HashSet<>();
            }

            void increment() {
                counter++;
            }

            void reset() {
                counter = 0;
                writers.clear();
            }

            void trackAll(Set<StructLike> partitions) {
                writers.addAll(partitions);
            }

            Set<StructLike> getTrackedWriters() {
                return writers;
            }

            boolean isOverCountThreshold() {
                return counter >= countThreshold;
            }

            boolean shouldFlush() {
                return !writers.isEmpty();
            }

            @Override
            public String toString() {
                return "Trigger{"
                        + " countThreshold=" + countThreshold
                        + ", writers=" + writers
                        + ", counter=" + counter
                        + '}';
            }
        }
    }
}
