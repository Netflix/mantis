package io.mantisrx.server.worker.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import java.util.concurrent.atomic.AtomicBoolean;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.client.ConnectionPool;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.client.RxClient;
import mantis.io.reactivex.netty.metrics.MetricEventsListener;
import mantis.io.reactivex.netty.metrics.MetricEventsSubject;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient.HttpClientConfig.Builder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientImpl;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;

public class MantisHttpClientImpl<I, O> extends HttpClientImpl<I, O> implements RxClient<HttpClientRequest<I>, HttpClientResponse<O>> {

    protected final String name;
    protected final ServerInfo serverInfo;
    protected final Bootstrap clientBootstrap;
    protected final PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator;
    protected final ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> channelFactory;
    protected final ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionFactory;
    protected final ClientConfig clientConfig;
    protected final MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject;
    protected final ConnectionPool<HttpClientResponse<O>, HttpClientRequest<I>> pool;
    private final AtomicBoolean isShutdown = new AtomicBoolean();

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> channelFactory, ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<O, I>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, channelFactory, connectionFactory, eventsSubject);
        if (null == name) {
            throw new NullPointerException("Name can not be null.");
        } else if (null == clientBootstrap) {
            throw new NullPointerException("Client bootstrap can not be null.");
        } else if (null == serverInfo) {
            throw new NullPointerException("Server info can not be null.");
        } else if (null == clientConfig) {
            throw new NullPointerException("Client config can not be null.");
        } else if (null == connectionFactory) {
            throw new NullPointerException("Connection factory can not be null.");
        } else if (null == channelFactory) {
            throw new NullPointerException("Channel factory can not be null.");
        } else {
            this.name = name;
            this.pool = null;
            this.eventsSubject = eventsSubject;
            this.clientConfig = clientConfig;
            this.serverInfo = serverInfo;
            this.clientBootstrap = clientBootstrap;
            this.connectionFactory = connectionFactory;
            this.connectionFactory.useMetricEventsSubject(eventsSubject);
            this.channelFactory = channelFactory;
            this.channelFactory.useMetricEventsSubject(eventsSubject);
            this.pipelineConfigurator = pipelineConfigurator;
            final PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> configurator = this.adaptPipelineConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
            this.clientBootstrap.handler(new ChannelInitializer<Channel>() {
                public void initChannel(Channel ch) throws Exception {
                    configurator.configureNewPipeline(ch.pipeline());
                }
            });
        }
    }

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<HttpClientResponse<O>, HttpClientRequest<I>> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, poolBuilder, eventsSubject);
        if (null == name) {
            throw new NullPointerException("Name can not be null.");
        } else if (null == clientBootstrap) {
            throw new NullPointerException("Client bootstrap can not be null.");
        } else if (null == serverInfo) {
            throw new NullPointerException("Server info can not be null.");
        } else if (null == clientConfig) {
            throw new NullPointerException("Client config can not be null.");
        } else if (null == poolBuilder) {
            throw new NullPointerException("Pool builder can not be null.");
        } else {
            this.name = name;
            this.eventsSubject = eventsSubject;
            this.clientConfig = clientConfig;
            this.serverInfo = serverInfo;
            this.clientBootstrap = clientBootstrap;
            this.pipelineConfigurator = pipelineConfigurator;
            final PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> configurator = this.adaptPipelineConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
            this.clientBootstrap.handler(new ChannelInitializer<Channel>() {
                public void initChannel(Channel ch) throws Exception {
                    configurator.configureNewPipeline(ch.pipeline());
                }
            });
            this.pool = poolBuilder.build();
            this.channelFactory = poolBuilder.getChannelFactory();
            this.connectionFactory = poolBuilder.getConnectionFactory();
        }
    }


    public Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> getConnectionObservable() {
        if (this.isShutdown.get()) {
            return Observable.error(new IllegalStateException("Client is already shutdown."));
        } else {
            Observable toReturn;
            if (null != this.pool) {
                toReturn = this.pool.acquire();
            } else {
                toReturn = Observable.create(new OnSubscribe<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>>() {
                    public void call(Subscriber<? super ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> subscriber) {
                        try {
                            MantisHttpClientImpl.this.channelFactory.connect(subscriber, MantisHttpClientImpl.this.serverInfo, MantisHttpClientImpl.this.connectionFactory);
                            // call another method with the ChannelFuture object to expose it
                        } catch (Throwable var3) {
                            subscriber.onError(var3);
                        }

                    }
                });
            }

            return toReturn.take(1);
        }
    }

    public void shutdown() {
        if (this.isShutdown.compareAndSet(false, true)) {
            if (null != this.pool) {
                this.pool.shutdown();
            }

        }
    }

    public String name() {
        return this.name;
    }

    @Override
    protected PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> adaptPipelineConfigurator(PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        return PipelineConfigurators.createClientConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
    }

    public Subscription subscribe(MetricEventsListener<? extends ClientMetricsEvent<?>> listener) {
        return this.eventsSubject.subscribe(listener);
    }

    public Observable<HttpClientResponse<O>> submit(HttpClientRequest<I> request) {
        return this.submit(request, this.getConnectionObservable());
    }

    public Observable<HttpClientResponse<O>> submit(HttpClientRequest<I> request, ClientConfig config) {
        return this.submit(request, this.getConnectionObservable(), config);
    }

    public Observable<HttpClientResponse<O>> submit(HttpClientRequest<I> request, Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionObservable) {
        return super.submit(request, connectionObservable, (ClientConfig)(null == this.clientConfig ? Builder.newDefaultConfig() : this.clientConfig));
    }

}
