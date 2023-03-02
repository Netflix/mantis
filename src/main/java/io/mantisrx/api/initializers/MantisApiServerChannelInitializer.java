/*
 * Copyright 2018 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package io.mantisrx.api.initializers;

import com.netflix.netty.common.HttpLifecycleChannelHandler;
import com.netflix.netty.common.channel.config.ChannelConfig;
import com.netflix.netty.common.channel.config.CommonChannelConfigKeys;
import com.netflix.zuul.netty.server.BaseZuulChannelInitializer;
import com.netflix.zuul.netty.ssl.SslContextFactory;
import io.mantisrx.api.Util;
import io.mantisrx.api.push.ConnectionBroker;
import io.mantisrx.api.push.MantisSSEHandler;
import io.mantisrx.api.push.MantisWebSocketFrameHandler;
import io.mantisrx.api.tunnel.CrossRegionHandler;
import io.mantisrx.api.tunnel.MantisCrossRegionalClient;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.util.List;
import javax.net.ssl.SSLException;
import rx.Scheduler;


public class MantisApiServerChannelInitializer extends BaseZuulChannelInitializer
{
    private final SslContextFactory sslContextFactory;
    private final SslContext sslContext;
    private final boolean isSSlFromIntermediary;

    private final ConnectionBroker connectionBroker;
    private final HighAvailabilityServices highAvailabilityServices;
    private final MantisCrossRegionalClient mantisCrossRegionalClient;
    private final Scheduler scheduler;
    private final List<String> pushPrefixes;
    private final boolean sslEnabled;

    public MantisApiServerChannelInitializer(
            String metricId,
            ChannelConfig channelConfig,
            ChannelConfig channelDependencies,
            ChannelGroup channels,
            List<String> pushPrefixes,
            HighAvailabilityServices highAvailabilityServices,
            MantisCrossRegionalClient mantisCrossRegionalClient,
            ConnectionBroker connectionBroker,
            Scheduler scheduler,
            boolean sslEnabled) {
        super(metricId, channelConfig, channelDependencies, channels);

        this.pushPrefixes = pushPrefixes;
        this.connectionBroker = connectionBroker;
        this.highAvailabilityServices = highAvailabilityServices;
        this.mantisCrossRegionalClient = mantisCrossRegionalClient;
        this.scheduler = scheduler;
        this.sslEnabled = sslEnabled;

        this.isSSlFromIntermediary = channelConfig.get(CommonChannelConfigKeys.isSSlFromIntermediary);
        this.sslContextFactory = channelConfig.get(CommonChannelConfigKeys.sslContextFactory);

        if (sslEnabled) {
            try {
                sslContext = sslContextFactory.createBuilderForServer().build();
            } catch (SSLException e) {
                throw new RuntimeException("Error configuring SslContext!", e);
            }

            // Enable TLS Session Tickets support.
            sslContextFactory.enableSessionTickets(sslContext);

            // Setup metrics tracking the OpenSSL stats.
            sslContextFactory.configureOpenSslStatsMetrics(sslContext, metricId);
        } else {
            sslContext = null;
        }
    }




    @Override
    protected void initChannel(Channel ch) throws Exception
    {

        // Configure our pipeline of ChannelHandlerS.
        ChannelPipeline pipeline = ch.pipeline();

        storeChannel(ch);
        addTimeoutHandlers(pipeline);
        addPassportHandler(pipeline);
        addTcpRelatedHandlers(pipeline);

        if (sslEnabled) {
            SslHandler sslHandler = sslContext.newHandler(ch.alloc());
            sslHandler.engine().setEnabledProtocols(sslContextFactory.getProtocols());
            pipeline.addLast("ssl", sslHandler);
            addSslInfoHandlers(pipeline, isSSlFromIntermediary);
            addSslClientCertChecks(pipeline);
        }

        addHttp1Handlers(pipeline);
        addHttpRelatedHandlers(pipeline);

        pipeline.addLast("mantishandler", new MantisChannelHandler(pushPrefixes));
    }

    /**
     * Adds a series of handlers for providing SSE/Websocket connections
     * to Mantis Jobs.
     *
     * @param pipeline The netty pipeline to which push handlers should be added.
     * @param url The url with which to initiate the websocket handler.
     */
    protected void addPushHandlers(final ChannelPipeline pipeline, String url) {
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(new HttpObjectAggregator(64 * 1024));
        pipeline.addLast(new MantisSSEHandler(connectionBroker, highAvailabilityServices, pushPrefixes));
        pipeline.addLast(new WebSocketServerProtocolHandler(url, true));
        pipeline.addLast(new MantisWebSocketFrameHandler(connectionBroker));
    }

    /**
     * Adds a series of handlers for providing SSE/Websocket connections
     * to Mantis Jobs.
     *
     * @param pipeline The netty pipeline to which regional handlers should be added.
     */
    protected void addRegionalHandlers(final ChannelPipeline pipeline) {
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(new HttpObjectAggregator(10 * 1024 * 1024));
        pipeline.addLast(new CrossRegionHandler(pushPrefixes, mantisCrossRegionalClient, connectionBroker, scheduler));
    }

    /**
     * The MantisChannelHandler's job is to initialize the tail end of the pipeline differently
     * depending on the URI of the request. This is largely to circumvent issues with endpoint responses
     * when the push handlers preceed the Zuul handlers.
     */
    @Sharable
    public class MantisChannelHandler extends ChannelInboundHandlerAdapter {

        private final List<String> pushPrefixes;

        public MantisChannelHandler(List<String> pushPrefixes) {
            this.pushPrefixes = pushPrefixes;
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof HttpLifecycleChannelHandler.StartEvent) {
                HttpLifecycleChannelHandler.StartEvent startEvent = (HttpLifecycleChannelHandler.StartEvent) evt;
                String uri = startEvent.getRequest().uri();
                ChannelPipeline pipeline = ctx.pipeline();

                removeEverythingAfterThis(pipeline);

                if (Util.startsWithAnyOf(uri, this.pushPrefixes)) {
                    addPushHandlers(pipeline, uri);
                } else if(uri.startsWith("/region/")) {
                    addRegionalHandlers(pipeline);
                } else {
                    addZuulHandlers(pipeline);
                }
            }
            ctx.fireUserEventTriggered(evt);
        }
    }

    private void removeEverythingAfterThis(ChannelPipeline pipeline) {
        while (pipeline.last().getClass() != MantisChannelHandler.class) {
            pipeline.removeLast();
        }
    }
}
