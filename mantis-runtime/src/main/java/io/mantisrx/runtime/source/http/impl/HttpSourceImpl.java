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

package io.mantisrx.runtime.source.http.impl;

import static com.mantisrx.common.utils.MantisMetricStringConstants.DROP_OPERATOR_INCOMING_METRIC_GROUP;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_ESTABLISHED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_UNSUBSCRIBED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SERVER_FOUND;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SOURCE_COMPLETED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SUBSCRIPTION_ENDED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SUBSCRIPTION_ESTABLISHED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SUBSCRIPTION_FAILED;

import com.mantisrx.common.utils.NettyUtils;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import io.mantisrx.runtime.source.http.ClientResumePolicy;
import io.mantisrx.runtime.source.http.HttpClientFactory;
import io.mantisrx.runtime.source.http.HttpRequestFactory;
import io.mantisrx.runtime.source.http.HttpServerProvider;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType;
import io.mantisrx.server.core.ServiceRegistry;
import io.netty.util.ReferenceCountUtil;
import io.reactivx.mantis.operators.DropOperator;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import lombok.Value;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.Operator;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.PublishSubject;
import rx.subscriptions.Subscriptions;

/**
 * An HTTP source that connects to multiple servers, and streams responses from the servers with a single merged
 * stream.
 *
 * @param <R> The entity type of the request
 * @param <E> The entity type of the response
 */
public class HttpSourceImpl<R, E, T> implements Source<T> {

    private static final String DEFAULT_BUFFER_SIZE = "0";

    private static Logger logger = LoggerFactory.getLogger(HttpSourceImpl.class);

    static {
        NettyUtils.setNettyThreads();
    }

    private final HttpRequestFactory<R> requestFactory;
    private final HttpServerProvider serverProvider;
    // Note the HTTP source needs a client factory instead of using RxNetty's HttpClientBuilder directory
    // because we need to create a new HttpClientBuilder for each server to connect to. Therefore,
    // HttpSource can't take a single HttpClientBuilder.
    private final HttpClientFactory<R, E> clientFactory;
    private final Observer<HttpSourceEvent> observer;
    private final Func2<ServerContext<HttpClientResponse<E>>, E, T> postProcessor;
    private final ClientResumePolicy<R, E> resumePolicy;
    private final PublishSubject<ServerInfo> serversToRemove;
    private final Gauge connectionGauge;
    private final Gauge retryListGauge;
    private final Gauge connectionAttemptedGauge;
    private final Counter connectionEstablishedCounter;
    private final Counter connectionUnsubscribedCounter;
    private final Counter sourceCompletedCounter;
    private final Counter subscriptionEndedCounter;
    private final Counter subscriptionEstablishedCounter;
    private final Counter subscriptionFailedCounter;
    private final Counter serverFoundCounter;
    private final Counter subscriptionCancelledCounter;
    private final Counter dropped;
    //aggregated metrics for all connections to source servers
    private final Metrics incomingDataMetrics;
    private final ConnectionManager<E> connectionManager = new ConnectionManager<>();
    private final int bufferSize;
    private final Subscription serversToRemoveSubscription;

    /**
     * Constructs an {@code HttpSource} instance that is ready to connects to one or more servers provided by the given
     * {@link io.mantisrx.runtime.source.http.HttpServerProvider} instance.
     *
     * @param requestFactory A factory that creates a new request for the source to submit
     * @param serverProvider The provider that specifies with servers to connect to and which servers to disconnect
     *                       from
     * @param clientFactory  The factory that creates HTTP client for the source to make connections to each server
     * @param observer       The observer that gets notified for internal events of the source
     * @param resumePolicy   The policy of resuming client response when a response terminates
     */
    HttpSourceImpl(
            HttpRequestFactory<R> requestFactory,
            HttpServerProvider serverProvider,
            HttpClientFactory<R, E> clientFactory,
            Observer<HttpSourceEvent> observer,
            Func2<ServerContext<HttpClientResponse<E>>, E, T> postProcessor,
            ClientResumePolicy<R, E> resumePolicy) {
        this.requestFactory = requestFactory;
        this.serverProvider = serverProvider;
        this.clientFactory = clientFactory;
        this.observer = observer;
        this.postProcessor = postProcessor;
        this.resumePolicy = resumePolicy;

        Metrics m = new Metrics.Builder()
                .name(HttpSourceImpl.class.getCanonicalName())
                .addGauge("connectionGauge")
                .addGauge("retryListGauge")
                .addGauge("connectionAttemptedGauge")
                .addCounter("connectionEstablishedCounter")
                .addCounter("connectionUnsubscribedCounter")
                .addCounter("sourceCompletedCounter")
                .addCounter("subscriptionEndedCounter")
                .addCounter("subscriptionEstablishedCounter")
                .addCounter("subscriptionFailedCounter")
                .addCounter("serverFoundCounter")
                .addCounter("subscriptionCancelledCounter")
                .build();

        m = MetricsRegistry.getInstance().registerAndGet(m);

        connectionGauge = m.getGauge("connectionGauge");
        retryListGauge = m.getGauge("retryListGauge");
        connectionAttemptedGauge = m.getGauge("connectionAttemptedGauge");
        connectionEstablishedCounter = m.getCounter("connectionEstablishedCounter");
        connectionUnsubscribedCounter = m.getCounter("connectionUnsubscribedCounter");
        sourceCompletedCounter = m.getCounter("sourceCompletedCounter");
        subscriptionEndedCounter = m.getCounter("subscriptionEndedCounter");
        subscriptionEstablishedCounter = m.getCounter("subscriptionEstablishedCounter");
        subscriptionFailedCounter = m.getCounter("subscriptionFailedCounter");
        serverFoundCounter = m.getCounter("serverFoundCounter");
        subscriptionCancelledCounter = m.getCounter("subscriptionCancelledCounter");

        incomingDataMetrics = new Metrics.Builder()
                .name(DROP_OPERATOR_INCOMING_METRIC_GROUP + "_HttpSourceImpl")
                .addCounter("onNext")
                .addCounter("onError")
                .addCounter("onComplete")
                .addGauge("subscribe")
                .addCounter("dropped")
                .addGauge("requested")
                .addGauge("bufferedGauge")

                .build();

        MetricsRegistry.getInstance().registerAndGet(incomingDataMetrics);

        dropped = incomingDataMetrics.getCounter("dropped");

        String bufferSizeStr = ServiceRegistry.INSTANCE.getPropertiesService()
                .getStringValue("httpSource.buffer.size", DEFAULT_BUFFER_SIZE);
        bufferSize = Integer.parseInt(bufferSizeStr);

        // We use a subject here instead of directly using the observable of
        // servers to be removed because we do not want to complete the observable, or
        // the source will be completed.
        this.serversToRemove = PublishSubject.create();
        serversToRemoveSubscription =
            serverProvider
                .getServersToRemove()
                .subscribe(serversToRemove::onNext);
    }

    public static <R, E, T> Builder<R, E, T> builder(
            HttpClientFactory<R, E> clientFactory,
            HttpRequestFactory<R> requestFactory,
            Func2<ServerContext<HttpClientResponse<E>>, E, T> postProcessor) {
        return new Builder<>(clientFactory, requestFactory, postProcessor);
    }

    public static <R, E, T> Builder<R, E, T> builder(
            HttpClientFactory<R, E> clientFactory,
            HttpRequestFactory<R> requestFactory,
            Func2<ServerContext<HttpClientResponse<E>>, E, T> postProcessor,
            ClientResumePolicy<R, E> resumePolicy) {
        return new Builder<>(clientFactory, requestFactory, postProcessor, resumePolicy);
    }

    public static <E> Func2<ServerContext<HttpClientResponse<E>>, E, ServerContext<E>> contextWrapper() {
        return new Func2<ServerContext<HttpClientResponse<E>>, E, ServerContext<E>>() {
            @Override
            public ServerContext<E> call(ServerContext<HttpClientResponse<E>> context, E e) {
                return new ServerContext<>(context.getServer(), e);
            }
        };
    }

    public static <E> Func2<ServerContext<HttpClientResponse<E>>, E, E> identityConverter() {
        return new Func2<ServerContext<HttpClientResponse<E>>, E, E>() {
            @Override
            public E call(ServerContext<HttpClientResponse<E>> httpClientResponseServerContext, E e) {
                return e;
            }
        };
    }

    @Override
    public Observable<Observable<T>> call(Context context, Index index) {
        return serverProvider
                .getServersToAdd()
                .filter((ServerInfo serverInfo) -> !connectionManager.alreadyConnected(serverInfo) && !connectionManager
                        .connectionAlreadyAttempted(serverInfo))
                .flatMap((ServerInfo serverInfo) -> {
                    return streamServers(Observable.just(serverInfo));
                })
                .doOnError((Throwable error) -> {
                    logger.error(String.format("The source encountered an error " + error.getMessage(), error));
                    observer.onError(error);
                })
                .doAfterTerminate(() -> {
                    observer.onCompleted();
                    connectionManager.reset();
                })

                .lift(new Operator<Observable<T>, Observable<T>>() {
                    @Override
                    public Subscriber<? super Observable<T>> call(Subscriber<? super Observable<T>> subscriber) {
                        subscriber.add(Subscriptions.create(new Action0() {
                            @Override
                            public void call() {
                                // When there is no subscriber left, we should clean up everything
                                // so the next incoming request can be handled properly. The most
                                // important thing is cached connections. Without them being removed,
                                // this HttpSource will not accept new request. See filter(disconnectedServer())
                                // above.
                                connectionManager.reset();
                            }
                        }));

                        return subscriber;
                    }
                });
        // We must ref count this stream because we'd like to ensure that the stream cleans up
        // its internal states only if all subscribers are gone, and the stream is unsubscribed
        // only once instead of per un-subscription.
        // Should not have to share as we are doing it at the stage level.
        //               .share()
        //              .lift(new DropOperator<Observable<T>>("http_source_impl_share"));
    }

    @Override
    public void close() throws IOException {
        serversToRemoveSubscription.unsubscribe();
        connectionManager.reset();
    }

    private Observable<Observable<T>> streamServers(Observable<ServerInfo> servers) {
        return servers.map((ServerInfo server) -> {
            SERVER_FOUND.newEvent(observer, server);
            serverFoundCounter.increment();
            return new ServerClientContext<>(server, clientFactory.createClient(server), requestFactory, observer);
        })
                .flatMap((final ServerClientContext<R, E> clientContext) -> {
                    final Observable<HttpClientResponse<E>> response =
                            streamResponseUntilServerIsRemoved(clientContext);

                    return response
                            .map(response1 -> {
                                // We delay the event until it here because we need to make sure
                                // the response observable is not completed for any reason
                                CONNECTION_ESTABLISHED.newEvent(observer, clientContext.getServer());
                                connectionEstablishedCounter.increment();
                                return new ServerContext<>(clientContext.getServer(), new ClientWithResponse<>(clientContext.getClient(), response1));
                            })
                            .lift((Operator<ServerContext<ClientWithResponse<R, E>>, ServerContext<ClientWithResponse<R, E>>>) subscriber -> {
                                subscriber.add(Subscriptions.create(new Action0() {
                                    @Override
                                    public void call() {
                                        // Note a connection is not equivalent to a subscription. A subscriber subscribes to
                                        // to valid connection, while a connection may return server errors.
                                        connectionUnsubscribedCounter.increment();
                                        CONNECTION_UNSUBSCRIBED.newEvent(observer, clientContext.getServer());
                                    }
                                }));

                                return subscriber;
                            });
                })
                .map(new Func1<ServerContext<ClientWithResponse<R, E>>, Observable<T>>() {

                    @Override
                    public Observable<T> call(
                            final
                            ServerContext<ClientWithResponse<R, E>> context) {
                        final HttpClientResponse<E> response = context.getValue().getResponse();

                        final ServerInfo server = context.getServer();
                        SUBSCRIPTION_ESTABLISHED.newEvent(observer, server);
                        subscriptionEstablishedCounter.increment();
                        connectionManager.serverConnected(server, context.getValue());
                        connectionGauge.set(getConnectedServers().size());

                        return streamResponseContent(server, response)
                                .map(new Func1<E, T>() {
                                    @Override
                                    public T call(E e) {
                                        ReferenceCountUtil.retain(e);
                                        return postProcessor.call(context.map(c -> c.getResponse()), e);
                                    }
                                })
                                .lift(new DropOperator<T>(incomingDataMetrics))
                                .lift(new Operator<T, T>() {
                                    @Override
                                    public Subscriber<? super T> call(Subscriber<? super T> subscriber) {
                                        subscriber.add(Subscriptions.create(new Action0() {
                                            @Override
                                            public void call() {
                                                SUBSCRIPTION_ENDED.newEvent(observer, context.getServer());
                                                subscriptionEndedCounter.increment();
                                            }
                                        }));

                                        return subscriber;
                                    }
                                });
                    }
                });
    }

    //	private Func1<ServerInfo, Boolean> disconnectedServer() {
    //		return new Func1<ServerInfo, Boolean>() {
    //			@Override
    //			public Boolean call(ServerInfo serverInfo) {
    //				return !connectionManager.alreadyConnected(serverInfo);
    //			}
    //		};
    //	}

    private void checkResponseIsSuccessful(HttpClientResponse<E> response) {
        int status = response.getStatus().code();
        if (status != 200) {
            throw new RuntimeException(String.format(
                    "Expected 200 but got status %d and reason: %s",
                    status,
                    response.getStatus().reasonPhrase()));
        }
    }

    private Observable<E> streamResponseContent(final ServerInfo server, HttpClientResponse<E> response) {

        // Note we must unsubscribe from the content stream or the stream will continue receiving from server even if
        // the stream's response observable is unsubscribed.
        return response.getContent()

                .takeUntil(
                        serversToRemove.filter((ServerInfo toRemove) -> toRemove != null && toRemove.equals(server)))
                .doOnError((Throwable throwable) -> {
                    SUBSCRIPTION_FAILED.newEvent(observer, server);
                    subscriptionFailedCounter.increment();
                    retryListGauge.set(getRetryServers().size());
                    logger.info("server disconnected onError1: " + server);
                    connectionManager.serverDisconnected(server);
                    connectionGauge.set(getConnectedServers().size());
                })
                // Upon error, simply completes the observable for this particular server. The error should not be
                // propagated to the entire http source
                .onErrorResumeNext(Observable.empty())
                .doOnCompleted(() -> {
                    SOURCE_COMPLETED.newEvent(observer, server);
                    sourceCompletedCounter.increment();
                    logger.info("server disconnected onComplete1: " + server);
                    connectionManager.serverDisconnected(server);
                    retryListGauge.set(getRetryServers().size());
                    connectionGauge.set(getConnectedServers().size());
                });
    }

    private Observable<HttpClientResponse<E>> streamResponseUntilServerIsRemoved(final ServerClientContext<R, E> clientContext) {

        return clientContext
                .newResponse((ServerInfo t) -> {
                    connectionAttemptedGauge.set(getConnectionAttemptedServers().size());
                    connectionManager.serverConnectionAttempted(t);
                })
                .lift(new OperatorResumeOnError<>(new ResumeOnErrorPolicy<HttpClientResponse<E>>() {
                    @Override
                    public Observable<HttpClientResponse<E>> call(Integer attempts, Throwable error) {
                        return resumePolicy.onError(clientContext, attempts, error);
                    }
                }))
                .lift(new OperatorResumeOnCompleted<>(new ResumeOnCompletedPolicy<HttpClientResponse<E>>() {
                    @Override
                    public Observable<HttpClientResponse<E>> call(Integer attempts) {
                        return resumePolicy.onCompleted(clientContext, attempts);
                    }
                }))
                .takeUntil(
                        serversToRemove.filter((ServerInfo toRemove) -> {
                            boolean shouldUnsubscribe = toRemove != null && toRemove.equals(clientContext.getServer());
                            if (shouldUnsubscribe) {
                                subscriptionCancelledCounter.increment();
                                EventType.SUBSCRIPTION_CANCELED.newEvent(observer, toRemove);
                            }
                            return shouldUnsubscribe;
                        }).doOnNext((ServerInfo server) -> {
                            logger.info("server removed: " + server);
                            connectionManager.serverRemoved(server);
                            connectionGauge.set(getConnectedServers().size());
                        })
                )
                .doOnNext((HttpClientResponse<E> response) -> checkResponseIsSuccessful(response))
                .doOnError((Throwable error) -> {
                    logger.error(
                        "Connecting to server {} failed: {}",
                        clientContext.getServer(),
                        error.getMessage(),
                        error);
                    SUBSCRIPTION_FAILED.newEvent(observer, clientContext.getServer());
                    subscriptionFailedCounter.increment();
                    logger.info("server disconnected onError2: " + clientContext.getServer());
                    connectionManager.serverDisconnected(clientContext.getServer());
                    retryListGauge.set(getRetryServers().size());
                    connectionGauge.set(connectionManager.getConnectedServers().size());
                })
                .doOnCompleted(() -> {
                    // the response header obs completes here no op
                    //						connectionManager.serverDisconnected(clientContext.getServer());
                    //						logger.info("server disconnected onComplete2: " + clientContext.getServer());
                    //						connectionGauge.set(getConnectedServers().size());
                })
                // Upon error, simply completes the observable for this particular server. The error should not be
                // propagated to the entire http source
                .onErrorResumeNext(Observable.empty());
    }

    Set<ServerInfo> getConnectedServers() {
        return connectionManager.getConnectedServers();
    }

    Set<ServerInfo> getRetryServers() {
        return connectionManager.getRetryServers();
    }

    Set<ServerInfo> getConnectionAttemptedServers() {
        return connectionManager.getConnectionAttemptedServers();
    }

    public static class Builder<R, E, T> {

        public static final HttpServerProvider EMPTY_HTTP_SERVER_PROVIDER = new HttpServerProvider() {
            @Override
            public Observable<ServerInfo> getServersToAdd() {
                return Observable.empty();
            }

            @Override
            public Observable<ServerInfo> getServersToRemove() {
                return Observable.empty();
            }
        };

        private HttpRequestFactory<R> requestFactory;
        private HttpServerProvider serverProvider;
        private HttpClientFactory<R, E> httpClientFactory;
        private Observer<HttpSourceEvent> observer;
        private Func2<ServerContext<HttpClientResponse<E>>, E, T> postProcessor;
        private ClientResumePolicy<R, E> clientResumePolicy;

        public Builder(
                HttpClientFactory<R, E> clientFactory,
                HttpRequestFactory<R> requestFactory,
                Func2<ServerContext<HttpClientResponse<E>>, E, T> postProcessor
        ) {
            this.requestFactory = requestFactory;
            this.httpClientFactory = clientFactory;
            this.serverProvider = EMPTY_HTTP_SERVER_PROVIDER;
            this.postProcessor = postProcessor;

            // Do not resume by default
            this.clientResumePolicy = new ClientResumePolicy<R, E>() {
                @Override
                public Observable<HttpClientResponse<E>> onError(
                        ServerClientContext<R, E> clientContext,
                        int attempts,
                        Throwable error) {
                    return null;
                }

                @Override
                public Observable<HttpClientResponse<E>> onCompleted(
                        ServerClientContext<R, E> clientContext,
                        int attempts) {
                    return null;
                }
            };

            //			this.clientResumePolicy = ClientResumePolicies.maxRepeat(9);
            observer = PublishSubject.create();
        }

        public Builder(
                HttpClientFactory<R, E> clientFactory,
                HttpRequestFactory<R> requestFactory,
                Func2<ServerContext<HttpClientResponse<E>>, E, T> postProcessor,
                ClientResumePolicy<R, E> resumePolicy
        ) {
            this.requestFactory = requestFactory;
            this.httpClientFactory = clientFactory;
            this.serverProvider = EMPTY_HTTP_SERVER_PROVIDER;
            this.postProcessor = postProcessor;
            this.clientResumePolicy = resumePolicy;

            observer = PublishSubject.create();
        }

        public Builder<R, E, T> withServerProvider(HttpServerProvider serverProvider) {
            this.serverProvider = serverProvider;
            return this;
        }

        public Builder<R, E, T> withActivityObserver(Observer<HttpSourceEvent> observer) {
            this.observer = observer;

            return this;
        }

        public Builder<R, E, T> resumeWith(ClientResumePolicy<R, E> policy) {
            this.clientResumePolicy = policy;

            return this;
        }

        public HttpSourceImpl<R, E, T> build() {
            return new HttpSourceImpl<>(
                    requestFactory,
                    serverProvider,
                    httpClientFactory,
                    observer,
                    postProcessor,
                    clientResumePolicy);
        }
    }

    public static class HttpSourceEvent {

        private final ServerInfo server;
        private final EventType eventType;

        private HttpSourceEvent(ServerInfo server, EventType eventType) {
            this.server = server;
            this.eventType = eventType;
        }

        public ServerInfo getServer() {
            return server;
        }

        public EventType getEventType() {
            return eventType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HttpSourceEvent that = (HttpSourceEvent) o;

            if (eventType != that.eventType) {
                return false;
            }
            if (server != null ? !server.equals(that.server) : that.server != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = server != null ? server.hashCode() : 0;
            result = 31 * result + (eventType != null ? eventType.hashCode() : 0);
            return result;
        }

        public static enum EventType {
            SERVER_FOUND,
            CONNECTION_ATTEMPTED,
            CONNECTION_ESTABLISHED,
            CONNECTION_UNSUBSCRIBED,
            SOURCE_COMPLETED,
            SUBSCRIPTION_ESTABLISHED,
            SUBSCRIPTION_FAILED,
            SUBSCRIPTION_CANCELED,
            SUBSCRIPTION_ENDED;

            public HttpSourceEvent newEvent(Observer<HttpSourceEvent> observer, ServerInfo server) {
                HttpSourceEvent event = new HttpSourceEvent(server, this);
                observer.onNext(event);

                return event;
            }
        }
    }

    private static class ConnectionManager<E> {

        private final ConcurrentMap<ServerInfo, ClientWithResponse<?, E>> connectedServers = new ConcurrentHashMap<>();
        private final Set<ServerInfo> retryServers = new CopyOnWriteArraySet<>();
        private final Set<ServerInfo> connectionAttempted = new CopyOnWriteArraySet<>();

        //private final Set<ServerInfo> connectedServers = new CopyOnWriteArraySet<>();
        public void serverConnected(ServerInfo server, ClientWithResponse<?, E> response) {
            connectedServers.put(server, response);
            retryServers.remove(server);
            connectionAttempted.remove(server);
            logger.info("CM: Server connected: " + server + " count " + connectedServers.size());
        }

        public void serverConnectionAttempted(ServerInfo server) {
            connectionAttempted.add(server);
        }

        public boolean alreadyConnected(ServerInfo server) {
            //logger.info("CM: is connected? " + server);
            return connectedServers.containsKey(server);
        }

        public boolean connectionAlreadyAttempted(ServerInfo server) {
            //logger.info("CM: is connected? " + server);
            return connectionAttempted.contains(server);
        }

        public void serverDisconnected(ServerInfo server) {
            removeConnectedServer(server);
            connectionAttempted.remove(server);
            retryServers.add(server);
            logger.info("CM: Server disconnected: " + server + " count " + connectedServers.size());
        }

        public void serverRemoved(ServerInfo server) {

            removeConnectedServer(server);
            connectionAttempted.remove(server);
            retryServers.remove(server);
            logger.info("CM: Server removed: " + server + " count " + connectedServers.size());
        }

        private void removeConnectedServer(ServerInfo serverInfo) {
            ClientWithResponse<?, E> stored = connectedServers.remove(serverInfo);
            if (stored != null) {
                try {
                    stored.getClient().shutdown();
                } catch (Exception e) {
                    logger.error("Failed to shut the client for {} successfully", serverInfo, e);
                }
            }
        }

        public Set<ServerInfo> getConnectedServers() {
            return Collections.unmodifiableSet(connectedServers.keySet());
        }

        public Set<ServerInfo> getRetryServers() {
            return Collections.unmodifiableSet(retryServers);
        }

        public Set<ServerInfo> getConnectionAttemptedServers() {
            return Collections.unmodifiableSet(connectionAttempted);
        }

        public void reset() {
            Set<ServerInfo> connectedServerInfos = new HashSet<>(connectedServers.keySet());
            for (ServerInfo serverInfo: connectedServerInfos) {
                removeConnectedServer(serverInfo);
            }

            connectionAttempted.clear();
            retryServers.clear();
            logger.info("CM: reset");
        }
    }

    @Value
    static class ClientWithResponse<R, E> {
        HttpClient<R, E> client;
        HttpClientResponse<E> response;
    }
}
