package io.mantisrx.server.worker.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.client.RxClientImpl;
import mantis.io.reactivex.netty.metrics.MetricEventsSubject;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import rx.Observable;
import rx.Subscriber;
import rx.Observable.OnSubscribe;

public class MantisRxClient<I, O> extends RxClientImpl<I, O> {

    public MantisRxClient(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<O, I> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<O, I> channelFactory, ClientConnectionFactory<O, I, ? extends ObservableConnection<O, I>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, channelFactory, connectionFactory, eventsSubject);
    }

    public MantisRxClient(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<O, I> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<O, I> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, poolBuilder, eventsSubject);
    }

    public Observable<ObservableConnection<O, I>> connect(ChannelFuture channelFuture) {
        if (super.isShutdown.get()) {
            return Observable.error(new IllegalStateException("Client is already shutdown."));
        } else {
            Observable toReturn;
            if (null != this.pool) {
                toReturn = this.pool.acquire();
            } else {
                toReturn = Observable.create(new OnSubscribe<ObservableConnection<O, I>>() {
                    public void call(Subscriber<? super ObservableConnection<O, I>> subscriber) {
                        try {
                            ChannelFuture x = RxClientImpl.this.channelFactory.connect(subscriber, RxClientImpl.this.serverInfo, RxClientImpl.this.connectionFactory);
                        } catch (Throwable var3) {
                            subscriber.onError(var3);
                        }

                    }
                });
            }

            return toReturn.take(1);
        }
    }

    public ChannelFuture getChannelFuture() {
        return RxClientImpl.super.channelFactory.connect(subscriber, RxClientImpl.this.serverInfo, RxClientImpl.this.connectionFactory);
    }
}
