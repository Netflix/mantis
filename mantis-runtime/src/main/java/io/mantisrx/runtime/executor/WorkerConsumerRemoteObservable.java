/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.runtime.executor;

import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.runtime.*;
import io.reactivex.mantis.remote.observable.ConnectToGroupedObservable;
import io.reactivex.mantis.remote.observable.ConnectToObservable;
import io.reactivex.mantis.remote.observable.DynamicConnectionSet;
import io.reactivex.mantis.remote.observable.EndpointInjector;
import io.reactivex.mantis.remote.observable.reconciliator.Reconciliator;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class WorkerConsumerRemoteObservable<T, R> implements WorkerConsumer<T> {

    private static final Logger logger = LoggerFactory.getLogger(WorkerConsumerRemoteObservable.class);

    private final String name;
    private final EndpointInjector injector;

    private DynamicConnectionSet<T> connectionSet;
    private Reconciliator<T> reconciliator;

    public WorkerConsumerRemoteObservable(String name,
                                          EndpointInjector endpointInjector) {
        this.name = name;
        this.injector = endpointInjector;
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Override
    public Observable<Observable<T>> start(StageConfig<T, ?> stage) {
        if (stage instanceof KeyToKey || stage instanceof KeyToScalar || stage instanceof GroupToScalar || stage instanceof GroupToGroup) {
            logger.info("Remote connection to stage " + name + " is KeyedStage");
            ConnectToGroupedObservable.Builder connectToBuilder =
                    new ConnectToGroupedObservable.Builder()
                            .name(name)
                            // need to include index offset here
                            .keyDecoder(stage.getInputKeyCodec())
                            .valueDecoder(stage.getInputCodec())
                            .subscribeAttempts(30); // max retry before failure

            connectionSet = DynamicConnectionSet.createMGO(connectToBuilder);

        } else if (stage instanceof ScalarToScalar || stage instanceof ScalarToKey || stage instanceof ScalarToGroup) {

            logger.info("Remote connection to stage " + name + " is ScalarStage");
            ConnectToObservable.Builder connectToBuilder = new ConnectToObservable.Builder()
                    .name(name)
                    .decoder(stage.getInputCodec())
                    .subscribeAttempts(30); // max retry before failure

            connectionSet = DynamicConnectionSet.create(connectToBuilder);
        } else {
            throw new RuntimeException("Unsupported stage type: " + stage);
        }

        reconciliator = new Reconciliator.Builder()
                .name("worker2worker_" + name)
                .connectionSet(connectionSet)
                .injector(injector)
                .build();

        registerMetrics(reconciliator.getMetrics());
        registerMetrics(connectionSet.getConnectionMetrics());
        return reconciliator.observables();
    }

    private void registerMetrics(Metrics metrics) {
        MetricsRegistry.getInstance().registerAndGet(metrics);
    }

    @Override
    public void close() throws IOException {
    }

}
