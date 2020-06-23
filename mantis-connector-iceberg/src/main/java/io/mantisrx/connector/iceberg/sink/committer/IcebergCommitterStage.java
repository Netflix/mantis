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

package io.mantisrx.connector.iceberg.sink.committer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.connector.iceberg.sink.committer.config.CommitterProperties;
import io.mantisrx.connector.iceberg.sink.committer.metrics.CommitterMetrics;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.codec.JacksonCodecs;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
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
public class IcebergCommitterStage implements ScalarComputation<DataFile, Map<String, Object>> {

    private static final Logger logger = LoggerFactory.getLogger(IcebergCommitterStage.class);

    private final CommitterMetrics metrics;
    private final Schema schema;
    private final String[] tableIdentifierNames;

    private Transformer transformer;

    /**
     * Returns a config for this stage which has encoding/decoding semantics and parameter definitions.
     */
    public static ScalarToScalar.Config<DataFile, Map<String, Object>> config() {
        return new ScalarToScalar.Config<DataFile, Map<String, Object>>()
                .description("")
                .codec(JacksonCodecs.mapStringObject())
                .withParameters(parameters());
    }

    /**
     * Returns a list of parameter definitions for this stage.
     */
    public static List<ParameterDefinition<?>> parameters() {
        return Arrays.asList(
                new StringParameter().name(CommitterProperties.COMMIT_FREQUENCY_MS)
                        .description(CommitterProperties.COMMIT_FREQUENCY_DESCRIPTION)
                        .validator(Validators.alwaysPass())
                        .defaultValue(CommitterProperties.COMMIT_FREQUENCY_MS_DEFAULT)
                        .build()
        );
    }

    public IcebergCommitterStage(Schema schema, String... tableIdentifierNames) {
        this.metrics = new CommitterMetrics();
        this.schema = schema;
        this.tableIdentifierNames = tableIdentifierNames;
    }

    @Override
    public void init(Context context) {
        CommitterConfig config = new CommitterConfig(context.getParameters());
        Catalog catalog = context.getServiceLocator().service(Catalog.class);
        // TODO: Get namespace and name from config.
        TableIdentifier id = TableIdentifier.of(tableIdentifierNames);
        Table table = catalog.tableExists(id) ? catalog.loadTable(id) : catalog.createTable(id, schema);
        IcebergCommitter committer = new IcebergCommitter(metrics, config, table);
        transformer = new Transformer(config, committer, Schedulers.computation());
    }

    @Override
    public Observable<Map<String, Object>> call(Context context, Observable<DataFile> dataFileObservable) {
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
    public static class Transformer implements Observable.Transformer<DataFile, Map<String, Object>> {

        private final CommitterConfig config;
        private final IcebergCommitter committer;
        private final Scheduler scheduler;

        public Transformer(CommitterConfig config, IcebergCommitter committer, Scheduler scheduler) {
            this.config = config;
            this.committer = committer;
            this.scheduler = scheduler;
        }

        /**
         * Periodically commits DataFiles in a batch.
         */
        @Override
        public Observable<Map<String, Object>> call(Observable<DataFile> source) {
            return source
                    .buffer(config.getCommitFrequencyMs(), TimeUnit.MILLISECONDS, scheduler)
                    .filter(dataFiles -> !dataFiles.isEmpty())
                    .map(committer::commit)
                    .doOnNext(snapshot -> {
                        // metric
                    })
                    .onErrorResumeNext(throwable -> {
                        // metric
                        return Observable.empty();
                    });
        }
    }
}
