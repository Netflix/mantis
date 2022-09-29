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

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.properties.MantisPropertiesService;
import io.mantisrx.runtime.GroupToGroup;
import io.mantisrx.runtime.GroupToScalar;
import io.mantisrx.runtime.KeyToKey;
import io.mantisrx.runtime.KeyToScalar;
import io.mantisrx.runtime.ScalarToGroup;
import io.mantisrx.runtime.ScalarToKey;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.server.core.ServiceRegistry;
import io.reactivex.mantis.network.push.KeyValuePair;
import io.reactivex.mantis.network.push.LegacyTcpPushServer;
import io.reactivex.mantis.network.push.PushServers;
import io.reactivex.mantis.network.push.Routers;
import io.reactivex.mantis.network.push.ServerConfig;
import io.reactivex.mantis.remote.observable.RemoteRxServer;
import io.reactivex.mantis.remote.observable.RxMetrics;
import io.reactivex.mantis.remote.observable.ServeNestedObservable;
import io.reactivex.mantis.remote.observable.slotting.RoundRobin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

/**
 * Execution of WorkerPublisher that publishes the stream to the next stage.
 *
 * @param <T> incoming codec
 */
public class WorkerPublisherRemoteObservable<T> implements WorkerPublisher<T> {

    private static final Logger logger = LoggerFactory.getLogger(WorkerPublisherRemoteObservable.class);

    private final String name;
    private final int serverPort;
    private RemoteRxServer server;
    private final MantisPropertiesService propService;
    private String jobName;

    public WorkerPublisherRemoteObservable(int serverPort,
                                           String name, Observable<Integer> minConnectionsToSubscribe,
                                           String jobName) {
        this.name = name;
        this.serverPort = serverPort;
        this.propService = ServiceRegistry.INSTANCE.getPropertiesService();
        this.jobName = jobName;
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Override
    public void start(final StageConfig<?, T> stage, Observable<Observable<T>> toServe) {

        RemoteRxServer.Builder serverBuilder = new RemoteRxServer.Builder();

        if (stage instanceof KeyToKey || stage instanceof ScalarToKey || stage instanceof ScalarToGroup || stage instanceof GroupToGroup) {

            if (runNewW2WserverGroups(jobName)) {
                logger.info("Modern server setup for name: " + name + " type: Keyedstage");

                long expiryTimeInSecs = Long.MAX_VALUE;
                if (stage instanceof KeyToKey) {
                    expiryTimeInSecs = ((KeyToKey) stage).getKeyExpireTimeSeconds();
                } else if (stage instanceof ScalarToKey) {
                    expiryTimeInSecs = ((ScalarToKey) stage).getKeyExpireTimeSeconds();
                }

                Func1<T, byte[]> valueEncoder = t1 -> stage.getOutputCodec().encode(t1);

                Func1<String, byte[]> keyEncoder = t1 -> Codecs.string().encode(t1);


                ServerConfig<KeyValuePair<String, T>> config = new ServerConfig.Builder<KeyValuePair<String, T>>()
                        .name(name)
                        .port(serverPort)
                        .metricsRegistry(MetricsRegistry.getInstance())
                        .numQueueConsumers(numConsumerThreads())
                        .maxChunkSize(maxChunkSize())
                        .maxChunkTimeMSec(maxChunkTimeMSec())
                        .bufferCapacity(bufferCapacity())
                        .useSpscQueue(useSpsc())
                        .router(Routers.consistentHashingLegacyTcpProtocol(jobName, keyEncoder, valueEncoder))
                        .build();

                if (stage instanceof ScalarToGroup || stage instanceof GroupToGroup) {

                    final LegacyTcpPushServer<KeyValuePair<String, T>> modernServer =
                            PushServers.infiniteStreamLegacyTcpNestedMantisGroup(config, (Observable) toServe, expiryTimeInSecs, keyEncoder,
                                    io.reactivex.mantis.network.push.HashFunctions.ketama());

                    modernServer.start();

                    // support legacy server interface
                    this.server = new RemoteRxServer() {

                        @Override
                        public void start() {
                        }

                        @Override
                        public void startAndWait() {
                        }

                        @Override
                        public void shutdown() {
                            modernServer.shutdown();
                        }

                        @Override
                        public void blockUntilServerShutdown() {
                            modernServer.blockUntilShutdown();
                        }
                    };

                } else { // ScalarToKey or KeyTKey


                    final LegacyTcpPushServer<KeyValuePair<String, T>> modernServer =
                            PushServers.infiniteStreamLegacyTcpNestedGroupedObservable(config, (Observable) toServe, expiryTimeInSecs, keyEncoder,
                                    io.reactivex.mantis.network.push.HashFunctions.ketama());

                    modernServer.start();

                    // support legacy server interface
                    this.server = new RemoteRxServer() {

                        @Override
                        public void start() {
                        }

                        @Override
                        public void startAndWait() {
                        }

                        @Override
                        public void shutdown() {
                            modernServer.shutdown();
                        }

                        @Override
                        public void blockUntilServerShutdown() {
                            modernServer.blockUntilShutdown();
                        }
                    };
                }

            }
        } else if (stage instanceof ScalarToScalar || stage instanceof KeyToScalar || stage instanceof GroupToScalar) {

            if (runNewW2Wserver(jobName)) {
                logger.info("Modern server setup for name: " + name + " type: Scalarstage");

                Func1<T, byte[]> encoder = t1 -> stage.getOutputCodec().encode(t1);

                ServerConfig<T> config = new ServerConfig.Builder<T>()
                        .name(name)
                        .port(serverPort)
                        .metricsRegistry(MetricsRegistry.getInstance())
                        .router(Routers.roundRobinLegacyTcpProtocol(name, encoder))
                        .build();
                final LegacyTcpPushServer<T> modernServer =
                        PushServers.infiniteStreamLegacyTcpNested(config, toServe);
                modernServer.start();

                // support legacy server interface
                this.server = new RemoteRxServer() {

                    @Override
                    public void start() {
                    }

                    @Override
                    public void startAndWait() {
                    }

                    @Override
                    public void shutdown() {
                        modernServer.shutdown();
                    }

                    @Override
                    public void blockUntilServerShutdown() {
                        modernServer.blockUntilShutdown();
                    }
                };

            } else {
                logger.info("Legacy server setup for name: " + name + " type: Scalarstage");
                RoundRobin slotting = new RoundRobin();
                serverBuilder
                        .addObservable(new ServeNestedObservable.Builder()
                                .name(name)
                                .encoder(stage.getOutputCodec())
                                .observable(toServe)
                                // going up stream.
                                .slottingStrategy(slotting)
                                .build());
                MetricsRegistry.getInstance().registerAndGet(slotting.getMetrics());
                server = serverBuilder
                        .port(serverPort)
                        .build();
                server.start();
            }
        } else {
            throw new RuntimeException("Unsupported stage type: " + stage);
        }
    }

    private boolean useSpsc() {
        String stringValue = propService.getStringValue("mantis.w2w.spsc", "false");
        return Boolean.parseBoolean(stringValue);

    }

    private int bufferCapacity() {
        String stringValue = propService.getStringValue("mantis.w2w.toKeyBuffer", "50000");
        return Integer.parseInt(stringValue);
    }

    private int maxChunkTimeMSec() {
        String stringValue = propService.getStringValue("mantis.w2w.toKeyMaxChunkTimeMSec", "250");
        return Integer.parseInt(stringValue);
    }

    private int maxChunkSize() {
        String stringValue = propService.getStringValue("mantis.w2w.toKeyMaxChunkSize", "1000");
        return Integer.parseInt(stringValue);
    }

    private int numConsumerThreads() {
        // num threads to read/process from consumer queue
        String stringValue = propService.getStringValue("mantis.w2w.toKeyThreads", "1");
        return Integer.parseInt(stringValue);
    }

    private boolean runNewW2Wserver(String jobName) {
        String legacyServerString = propService.getStringValue("mantis.w2w.newServerImplScalar", "true");
        String legacyServerStringPerJob = propService.getStringValue(jobName + ".mantis.w2w.newServerImplScalar", "false");
        return Boolean.parseBoolean(legacyServerString) || Boolean.parseBoolean(legacyServerStringPerJob);
    }

    private boolean runNewW2WserverGroups(String jobName) {
        String legacyServerString = propService.getStringValue("mantis.w2w.newServerImplKeyed", "true");
        String legacyServerStringPerJob = propService.getStringValue(jobName + ".mantis.w2w.newServerImplKeyed", "false");
        return Boolean.parseBoolean(legacyServerString) || Boolean.parseBoolean(legacyServerStringPerJob);
    }

    @Override
    public void close() {
        server.shutdown();
    }

    public RemoteRxServer getServer() {
        return server;
    }

    @Override
    public RxMetrics getMetrics() {
        return server.getMetrics();
    }
}
