package io.mantisrx.server.worker.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import java.util.concurrent.atomic.AtomicBoolean;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.client.ConnectionPool;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.client.RxClient;
import mantis.io.reactivex.netty.client.RxClient.ClientConfig;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.metrics.MetricEventsListener;
import mantis.io.reactivex.netty.metrics.MetricEventsSubject;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.Observable.OnSubscribe;

public class MantisRxClient<I, O> implements RxClient<I, O> {
    protected final String name;
    protected final ServerInfo serverInfo;
    protected final Bootstrap clientBootstrap;
    protected final PipelineConfigurator<O, I> pipelineConfigurator;
    protected final ClientChannelFactory<O, I> channelFactory;
    protected final ClientConnectionFactory<O, I, ? extends ObservableConnection<O, I>> connectionFactory;
    protected final ClientConfig clientConfig;
    protected final MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject;
    protected final ConnectionPool<O, I> pool;
    private final AtomicBoolean isShutdown = new AtomicBoolean();

    public MantisRxClient(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<O, I> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<O, I> channelFactory, ClientConnectionFactory<O, I, ? extends ObservableConnection<O, I>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
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
            final PipelineConfigurator<O, I> configurator = this.adaptPipelineConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
            this.clientBootstrap.handler(new ChannelInitializer<Channel>() {
                public void initChannel(Channel ch) throws Exception {
                    configurator.configureNewPipeline(ch.pipeline());
                }
            });
        }
    }

    public MantisRxClient(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<O, I> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<O, I> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
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
            final PipelineConfigurator<O, I> configurator = this.adaptPipelineConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
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

    public Observable<ObservableConnection<O, I>> connect() {
        if (this.isShutdown.get()) {
            return Observable.error(new IllegalStateException("Client is already shutdown."));
        } else {
            Observable toReturn;
            if (null != this.pool) {
                toReturn = this.pool.acquire();
            } else {
                toReturn = Observable.create(new OnSubscribe<ObservableConnection<O, I>>() {
                    public void call(Subscriber<? super ObservableConnection<O, I>> subscriber) {
                        try {
                            MantisRxClient.this.channelFactory.connect(subscriber, MantisRxClient.this.serverInfo, MantisRxClient.this.connectionFactory);
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

    protected PipelineConfigurator<O, I> adaptPipelineConfigurator(PipelineConfigurator<O, I> pipelineConfigurator, ClientConfig clientConfig, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        return PipelineConfigurators.createClientConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
    }

    public Subscription subscribe(MetricEventsListener<? extends ClientMetricsEvent<?>> listener) {
        return this.eventsSubject.subscribe(listener);
    }
}
