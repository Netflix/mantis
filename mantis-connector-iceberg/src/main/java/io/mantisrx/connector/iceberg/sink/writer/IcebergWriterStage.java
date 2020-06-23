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

import io.mantisrx.connector.iceberg.sink.codecs.IcebergCodecs;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties;
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.EnumParameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.exceptions.Exceptions;

/**
 * Processing stage which writes records to Iceberg through a backing file store.
 */
public class IcebergWriterStage implements ScalarComputation<Record, DataFile> {

    private static final Logger logger = LoggerFactory.getLogger(IcebergWriterStage.class);

    private final WriterMetrics metrics;
    private final Schema writerSchema;

    // TODO: Move to config.
    private final String[] tableIdentifierNames;

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
                new StringParameter().name(WriterProperties.WRITER_FILE_FORMAT)
                        .description(WriterProperties.WRITER_FILE_FORMAT_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(WriterProperties.WRITER_FILE_FORMAT_DEFAULT)
                        .build(),
                new StringParameter().name(WriterProperties.WRITER_PARTITION_KEY)
                        .description(WriterProperties.WRITER_PARTITION_KEY_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(WriterProperties.WRITER_PARTITION_KEY_DEFAULT)
                        .build(),
                new EnumParameter<>(WriterProperties.PartitionTransforms.class)
                        .name(WriterProperties.WRITER_PARTITION_KEY_TRANSFORM)
                        .description(WriterProperties.WRITER_PARTITION_KEY_TRANSFORM_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(WriterProperties.WRITER_PARTITION_KEY_TRANSFORM_DEFAULT)
                        .build()
        );
    }

    /**
     * Use this method to create an Iceberg Writer independent of a Mantis Processing Stage.
     *
     * This is useful for optimizing network utilization by using the writer directly within another
     * processing stage instead of having to traverse stage boundaries.
     *
     * This incurs a debuggability trade-off where a processing stage will do multiple things.
     */
    public static IcebergWriter newIcebergWriter(
            WriterConfig config,
            WriterMetrics metrics,
            WorkerInfo workerInfo,
            Table table,
            Schema writerSchema) {
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(writerSchema);

        // TODO: Support composite keys.
        String key = config.getWriterPartitionKey();
        switch (config.getWriterPartitionKeyTransform()) {
            case IDENTITY:
                specBuilder.identity(key);
                break;
            case YEAR:
                specBuilder.year(key);
                break;
            case MONTH:
                specBuilder.month(key);
                break;
            case DAY:
                specBuilder.day(key);
                break;
            case HOUR:
                specBuilder.hour(key);
                break;
            case NONE:
                // Unpartitioned writer.
            default:
                // Unpartitioned writer.
                break;
        }
        PartitionSpec spec = specBuilder.build();

        if (spec.fields().isEmpty()) {
            return new UnpartitionedIcebergWriter(metrics, config, workerInfo, table, spec);
        } else {
            return new PartitionedIcebergWriter(metrics, config, workerInfo, table, writerSchema, spec);
        }
    }

    public IcebergWriterStage(Schema writerSchema, String... tableIdentifierNames) {
        this.metrics = new WriterMetrics();
        this.writerSchema = writerSchema;
        this.tableIdentifierNames = tableIdentifierNames;
    }

    @Override
    public void init(Context context) {
        Configuration hadoopConfig = context.getServiceLocator().service(Configuration.class);
        WriterConfig config = new WriterConfig(context.getParameters(), hadoopConfig);
        Catalog catalog = context.getServiceLocator().service(Catalog.class);
        // TODO: Get namespace and name from config.
        TableIdentifier id = TableIdentifier.of(tableIdentifierNames);
        Table table = catalog.tableExists(id) ? catalog.loadTable(id) : catalog.createTable(id, writerSchema);
        WorkerInfo workerInfo = context.getWorkerInfo();

        IcebergWriter writer = newIcebergWriter(config, metrics, workerInfo, table, writerSchema);
        transformer = new Transformer(config, writer);
        try {
            writer.open();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file appender for Iceberg Writer", e);
        }
    }

    @Override
    public Observable<DataFile> call(Context context, Observable<Record> recordObservable) {
        return recordObservable.compose(transformer);
    }

    /**
     * Reactive Transformer for writing records to Iceberg.
     *
     * Users may use this class independently of this Stage, for example, if they want to
     * {@link Observable#compose(Observable.Transformer)} this transformer with a flow into
     * an existing Stage. One benefit of this co-location is to avoid extra network
     * cost from worker-to-worker communication, trading off debuggability.
     */
    public static class Transformer implements Observable.Transformer<Record, DataFile> {

        private final WriterConfig config;
        private final IcebergWriter writer;

        public Transformer(WriterConfig config, IcebergWriter writer) {
            this.config = config;
            this.writer = writer;
        }

        /**
         * Opens an IcebergWriter FileAppender, writes records to a file. Check the file appender
         * size on a configured count, and if over a configured threshold, close the file, build
         * and emit a DataFile, and open a new FileAppender.
         *
         * Pair this with a progressive multipart file uploader backend for better latencies.
         */
        @Override
        public Observable<DataFile> call(Observable<Record> source) {
            return source
                    .scan(new Counter(config.getWriterRowGroupSize()), (counter, record) -> {
                        writer.write(record);
                        counter.increment();
                        return counter;
                    })
                    .filter(Counter::shouldReset)
                    .filter(counter -> writer.length() >= config.getWriterFlushFrequencyBytes())
                    .map(counter -> {
                        try {
                            DataFile dataFile = writer.close();
                            writer.open();
                            counter.reset();
                            return dataFile;
                        } catch (IOException e) {
                            throw Exceptions.propagate(e);
                        }
                    })
                    .doOnNext(dataFile -> {
                        // metric
                    })
                    .onErrorResumeNext(error -> {
                        // metric
                        logger.error("error writing record", error);
                        return Observable.empty();
                    })
                    .doOnCompleted(() -> {
                        try {
                            writer.close();
                        } catch (IOException e) {
                            throw Exceptions.propagate(e);
                        }
                    });
        }
    }

    private static class Counter {

        private final int threshold;
        private int counter;

        Counter(int threshold) {
            this.threshold = threshold;
            this.counter = 0;
        }

        void increment() {
            counter++;
        }

        void reset() {
            counter = 0;
        }

        boolean shouldReset() {
            return counter >= threshold;
        }
    }
}
