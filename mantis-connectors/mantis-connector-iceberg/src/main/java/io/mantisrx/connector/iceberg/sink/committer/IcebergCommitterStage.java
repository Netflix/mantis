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

package io.mantisrx.connector.iceberg.sink.committer;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.connector.iceberg.sink.committer.config.CommitterProperties;
import io.mantisrx.connector.iceberg.sink.committer.metrics.CommitterMetrics;
import io.mantisrx.connector.iceberg.sink.committer.watermarks.WatermarkExtractor;
import io.mantisrx.connector.iceberg.sink.config.SinkProperties;
import io.mantisrx.connector.iceberg.sink.writer.MantisDataFile;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.codec.JacksonCodecs;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Processing stage which commits table metadata to Iceberg on a time interval.
 */
public class IcebergCommitterStage implements ScalarComputation<MantisDataFile, Map<String, Object>> {

    private static final Logger logger = LoggerFactory.getLogger(IcebergCommitterStage.class);

    private Transformer transformer;

    /**
     * Returns a config for this stage which has encoding/decoding semantics and parameter definitions.
     */
    public static ScalarToScalar.Config<MantisDataFile, Map<String, Object>> config() {
        return new ScalarToScalar.Config<MantisDataFile, Map<String, Object>>()
                .description("")
                .codec(JacksonCodecs.mapStringObject())
                .withParameters(parameters());
    }

    /**
     * Returns a list of parameter definitions for this stage.
     */
    public static List<ParameterDefinition<?>> parameters() {
        return Arrays.asList(
                new StringParameter().name(SinkProperties.SINK_CATALOG)
                        .description(SinkProperties.SINK_CATALOG_DESCRIPTION)
                        .validator(Validators.notNullOrEmpty())
                        .required()
                        .build(),
                new StringParameter().name(SinkProperties.SINK_DATABASE)
                        .description(SinkProperties.SINK_DATABASE_DESCRIPTION)
                        .validator(Validators.notNullOrEmpty())
                        .required()
                        .build(),
                new StringParameter().name(SinkProperties.SINK_TABLE)
                        .description(SinkProperties.SINK_TABLE_DESCRIPTION)
                        .validator(Validators.notNullOrEmpty())
                        .required()
                        .build(),
                new StringParameter().name(CommitterProperties.COMMIT_FREQUENCY_MS)
                        .description(CommitterProperties.COMMIT_FREQUENCY_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(CommitterProperties.COMMIT_FREQUENCY_MS_DEFAULT)
                        .build(),
                new StringParameter().name(CommitterProperties.WATERMARK_PROPERTY_KEY)
                    .description(CommitterProperties.WATERMARK_PROPERTY_DESCRIPTION)
                    .validator(Validators.alwaysPass())
                    .build()
        );
    }

    /**
     * Use this to instantiate a new transformer from a given {@link Context}.
     */
    public static Transformer newTransformer(Context context) {
        CommitterConfig config = new CommitterConfig(context.getParameters());
        CommitterMetrics metrics = new CommitterMetrics();
        Catalog catalog = context.getServiceLocator().service(Catalog.class);
        TableIdentifier id = TableIdentifier.of(config.getCatalog(), config.getDatabase(), config.getTable());
        Table table = catalog.loadTable(id);

        WatermarkExtractor watermarkExtractor = context.getServiceLocator().service(WatermarkExtractor.class);
        IcebergCommitter committer = new IcebergCommitter(table, config, watermarkExtractor);

        return new Transformer(config, metrics, committer, Schedulers.computation());
    }

    public IcebergCommitterStage() {
    }

    /**
     * Uses the provided Mantis Context to inject configuration and creates an underlying table appender.
     *
     * This method depends on a Hadoop Configuration and Iceberg Catalog, both injected
     * from the Context's service locator.
     *
     * Note that this method expects an Iceberg Table to have been previously created out-of-band,
     * otherwise initialization will fail. Users should prefer to create tables
     * out-of-band so they can be versioned alongside their schemas.
     */
    @Override
    public void init(Context context) {
        transformer = newTransformer(context);
    }

    @Override
    public Observable<Map<String, Object>> call(Context context, Observable<MantisDataFile> dataFileObservable) {
        return dataFileObservable.compose(transformer);
    }

    /**
     * Reactive Transformer for committing metadata to Iceberg.
     *
     * Users may use this class independently of this Stage, for example, if they want to
     * {@link Observable#compose(Observable.Transformer)} this transformer with a flow into
     * an existing Stage. One benefit of this co-location is to avoid extra network
     * cost from worker-to-worker communication, trading off debuggability.
     */
    public static class Transformer implements Observable.Transformer<MantisDataFile, Map<String, Object>> {

        private final CommitterConfig config;
        private final CommitterMetrics metrics;
        private final IcebergCommitter committer;
        private final Scheduler scheduler;

        public Transformer(CommitterConfig config,
                           CommitterMetrics metrics,
                           IcebergCommitter committer,
                           Scheduler scheduler) {
            this.config = config;
            this.metrics = metrics;
            this.committer = committer;
            this.scheduler = scheduler;
            logger.info("Initialized IcebergCommitterStage with config: {}", config);
        }

        /**
         * Periodically commits DataFiles to Iceberg as a batch.
         */
        @Override
        public Observable<Map<String, Object>> call(Observable<MantisDataFile> source) {
            return source
                    .buffer(config.getCommitFrequencyMs(), TimeUnit.MILLISECONDS, scheduler)
                    .doOnNext(dataFiles -> metrics.increment(CommitterMetrics.INVOCATION_COUNT))
                    .filter(dataFiles -> !dataFiles.isEmpty())
                    .map(dataFiles -> {
                        try {
                            long start = scheduler.now();
                            Map<String, Object> summary = committer.commit(dataFiles);
                            long now = scheduler.now();
                            metrics.record(CommitterMetrics.COMMIT_LATENCY_MSEC, now - start, TimeUnit.MILLISECONDS);
                            metrics.setGauge(CommitterMetrics.COMMIT_BATCH_SIZE, dataFiles.size());
                            return summary;
                        } catch (RuntimeException e) {
                            metrics.increment(CommitterMetrics.COMMIT_FAILURE_COUNT);
                            logger.error("error committing to Iceberg table {}.{}.{}",
                                config.getCatalog(),
                                config.getDatabase(),
                                config.getTable(),
                                e);
                            return new HashMap<String, Object>();
                        }
                    })
                    .filter(summary -> !summary.isEmpty())
                    .doOnNext(summary -> {
                        metrics.increment(CommitterMetrics.COMMIT_SUCCESS_COUNT);
                        logger.info("committed to table {}.{}.{} with summary: {}",
                            config.getCatalog(),
                            config.getDatabase(),
                            config.getTable(),
                            summary);
                    });
        }
    }
}
