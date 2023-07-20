package io.mantisrx.server.worker.client;

import io.netty.bootstrap.Bootstrap;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.metrics.MetricEventsSubject;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient.HttpClientConfig.Builder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientImpl;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;

public class MantisHttpClient<I, O> extends HttpClientImpl<HttpClientRequest<I>, HttpClientResponse<O>> {
    public MantisHttpClient(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<HttpClientResponse<O>>, HttpClientRequest<HttpClientRequest<I>>> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<HttpClientResponse<HttpClientResponse<O>>, HttpClientRequest<HttpClientRequest<I>>> channelFactory, ClientConnectionFactory<HttpClientResponse<HttpClientResponse<O>>, HttpClientRequest<HttpClientRequest<I>>, ? extends ObservableConnection<HttpClientResponse<HttpClientResponse<O>>, HttpClientRequest<HttpClientRequest<I>>>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, channelFactory, connectionFactory, eventsSubject);
    }

    public MantisHttpClient(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<HttpClientResponse<O>>, HttpClientRequest<HttpClientRequest<I>>> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<HttpClientResponse<HttpClientResponse<O>>, HttpClientRequest<HttpClientRequest<I>>> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, poolBuilder, eventsSubject);
    }

    public Observable<HttpClientResponse<HttpClientResponse<O>>> submitWrapper(HttpClientRequest<HttpClientRequest<I>> request, Observable<ObservableConnection<HttpClientResponse<HttpClientResponse<O>>, HttpClientRequest<HttpClientRequest<I>>>> connectionObservable) {
        return super.submit(request, connectionObservable, (ClientConfig)(null == this.clientConfig ? Builder.newDefaultConfig() : this.clientConfig));
    }
}