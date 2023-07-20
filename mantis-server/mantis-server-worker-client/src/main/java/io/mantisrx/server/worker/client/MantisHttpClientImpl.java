package io.mantisrx.server.worker.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.handler.codec.http.HttpMethod;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.client.ConnectionPool;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.metrics.Clock;
import mantis.io.reactivex.netty.metrics.MetricEventsSubject;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfiguratorComposite;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient.HttpClientConfig.Builder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient.HttpClientConfig.RedirectsHandling;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientMetricsEvent;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Action0;

public class MantisHttpClientImpl<I, O> extends MantisRxClientImpl<HttpClientRequest<I>, HttpClientResponse<O>> implements HttpClient<I, O> {
    private final String hostHeaderValue = this.prepareHostHeaderValue();

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> channelFactory, ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, channelFactory, connectionFactory, eventsSubject);
    }

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<HttpClientResponse<O>, HttpClientRequest<I>> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, poolBuilder, eventsSubject);
    }

    public Observable<HttpClientResponse<O>> submit(HttpClientRequest<I> request) {
        return this.submit(request, this.connect());
    }

    public Observable<HttpClientResponse<O>> submit(HttpClientRequest<I> request, ClientConfig config) {
        return this.submit(request, this.connect(), config);
    }

    protected Observable<HttpClientResponse<O>> submit(HttpClientRequest<I> request, Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionObservable) {
        return this.submit(request, connectionObservable, (ClientConfig)(null == this.clientConfig ? Builder.newDefaultConfig() : this.clientConfig));
    }

    protected Observable<HttpClientResponse<O>> submit(HttpClientRequest<I> request, Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionObservable, ClientConfig config) {
        final long startTimeMillis = Clock.newStartTimeMillis();
        HttpClientConfig httpClientConfig;
        if (config instanceof HttpClientConfig) {
            httpClientConfig = (HttpClientConfig)config;
        } else {
            httpClientConfig = new HttpClientConfig(config);
        }

        boolean followRedirect = this.shouldFollowRedirectForRequest(httpClientConfig, request);
        this.enrichRequest(request, httpClientConfig);
        Observable<HttpClientResponse<O>> toReturn = connectionObservable.lift(new RequestProcessingOperator(request, this.eventsSubject, httpClientConfig.getResponseSubscriptionTimeoutMs()));
        if (followRedirect) {
            toReturn = toReturn.lift(new RedirectOperator(request, this, httpClientConfig));
        }

        return toReturn.take(1).finallyDo(new Action0() {
            public void call() {
                mantis.io.reactivex.netty.protocol.http.client.HttpClientImpl.this.eventsSubject.onEvent(HttpClientMetricsEvent.REQUEST_PROCESSING_COMPLETE, Clock.onEndMillis(startTimeMillis));
            }
        });
    }

    protected PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> adaptPipelineConfigurator(PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        long responseSubscriptionTimeoutMs = 0L;
        if (clientConfig instanceof HttpClientConfig) {
            HttpClientConfig httpClientConfig = (HttpClientConfig)clientConfig;
            responseSubscriptionTimeoutMs = httpClientConfig.getResponseSubscriptionTimeoutMs();
        }

        PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> configurator = new PipelineConfiguratorComposite(new PipelineConfigurator[]{pipelineConfigurator, new ClientRequiredConfigurator(eventsSubject, responseSubscriptionTimeoutMs, TimeUnit.MILLISECONDS)});
        return super.adaptPipelineConfigurator(configurator, clientConfig, eventsSubject);
    }

    protected boolean shouldFollowRedirectForRequest(HttpClientConfig config, HttpClientRequest<I> request) {
        RedirectsHandling redirectsHandling = config.getFollowRedirect();
        switch(redirectsHandling) {
            case Enable:
                return true;
            case Disable:
                return false;
            case Undefined:
                return request.getMethod() == HttpMethod.HEAD || request.getMethod() == HttpMethod.GET;
            default:
                return false;
        }
    }

    ConnectionPool<HttpClientResponse<O>, HttpClientRequest<I>> getConnectionPool() {
        return this.pool;
    }

    private void enrichRequest(HttpClientRequest<I> request, ClientConfig config) {
        request.setDynamicUriParts(this.serverInfo.getHost(), this.serverInfo.getPort(), false);
        if (!request.getHeaders().contains("Host")) {
            request.getHeaders().add("Host", this.hostHeaderValue);
        }

        if (config instanceof HttpClientConfig) {
            HttpClientConfig httpClientConfig = (HttpClientConfig)config;
            if (httpClientConfig.getUserAgent() != null && request.getHeaders().get("User-Agent") == null) {
                request.getHeaders().set("User-Agent", httpClientConfig.getUserAgent());
            }
        }

    }

    private String prepareHostHeaderValue() {
        return this.serverInfo.getPort() != 80 && this.serverInfo.getPort() != 443 ? this.serverInfo.getHost() + ':' + this.serverInfo.getPort() : this.serverInfo.getHost();
    }
}
