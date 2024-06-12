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

package io.mantisrx.connector.publish.source.http;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.connector.publish.core.QueryRegistry;
import io.mantisrx.publish.proto.MantisServerSubscription;
import io.mantisrx.publish.proto.MantisServerSubscriptionEnvelope;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import java.util.List;
import mantis.io.reactivex.netty.protocol.http.server.UriInfoHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.subjects.Subject;


public class HttpSourceServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSourceServerHandler.class);

    private static final byte[] CONTENT = {'O', 'K'};
    private static final AsciiString CONTENT_TYPE = AsciiString.cached("Content-Type");
    private static final AsciiString CONTENT_LENGTH = AsciiString.cached("Content-Length");
    private static final AsciiString CONNECTION = AsciiString.cached("Connection");
    private static final AsciiString KEEP_ALIVE = AsciiString.cached("keep-alive");

    ObjectMapper mapper = new ObjectMapper();

    private final Counter getRequestCount;
    private final Counter unknownRequestCount;
    private final Counter postRequestCount;
    MetricGroupId metricGroupId;

    private final QueryRegistry registry;
    private final Subject<String, String> eventSubject;
    private final MeterRegistry meterRegistry;

    public HttpSourceServerHandler(QueryRegistry queryRegistry, Subject<String, String> eventSubject, MeterRegistry meterRegistry) {
        registry = queryRegistry;
        this.eventSubject = eventSubject;
        this.meterRegistry = meterRegistry;
//        metricGroupId = new MetricGroupId(SourceHttpServer.METRIC_GROUP + "_incoming");
        String groupName = SourceHttpServer.METRIC_GROUP + "_incoming";

        getRequestCount = meterRegistry.counter(groupName + "_" + "GetRequestCount");
        unknownRequestCount = meterRegistry.counter(groupName + "_" + "UnknownRequestCount");
        postRequestCount = meterRegistry.counter(groupName + "_" + "PostRequestCount");

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            boolean keepAlive = HttpUtil.isKeepAlive(req);
            if (req.method().equals(HttpMethod.GET)) {
                getRequestCount.increment();

                UriInfoHolder uriInfoHolder = new UriInfoHolder(req.uri());

                List<MantisServerSubscription> currentSubscriptions =
                        registry.getCurrentSubscriptions(uriInfoHolder.getQueryParameters());

                try {
                    byte[] serializedSubs =
                            mapper.writeValueAsBytes(new MantisServerSubscriptionEnvelope(currentSubscriptions));

                    FullHttpResponse response =
                            new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(serializedSubs));
                    response.headers().set(CONTENT_TYPE, "application/json");
                    response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());

                    if (!keepAlive) {
                        ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                    } else {
                        response.headers().set(CONNECTION, KEEP_ALIVE);
                        ctx.write(response);
                    }
                } catch (Exception e) {
                    LOGGER.error("problem reading from channel", e);
                }
            } else {
                if (req.method().equals(HttpMethod.POST)) {
                    postRequestCount.increment();
                    FullHttpMessage aggregator = (FullHttpMessage) msg;
                    ByteBuf content = aggregator.content();
                    String data = content.toString(CharsetUtil.UTF_8);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("got data " + data);
                    }
                    eventSubject.onNext(data);

                    FullHttpResponse response =
                            new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(CONTENT));
                    response.headers().set(CONTENT_TYPE, "text/plain");
                    response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());

                    if (!keepAlive) {
                        ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                    } else {
                        response.headers().set(CONNECTION, KEEP_ALIVE);
                        ctx.write(response);
                    }
                } else {
                    unknownRequestCount.increment();
                }
            }
        }
    }
}
