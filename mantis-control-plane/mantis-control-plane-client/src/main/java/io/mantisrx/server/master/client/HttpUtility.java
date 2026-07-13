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

package io.mantisrx.server.master.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.client.RxClient;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.protocol.http.client.CompositeHttpClientBuilder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;


/* package */ class HttpUtility {

    private static final Logger logger = LoggerFactory.getLogger(HttpUtility.class);
    private static final long GET_TIMEOUT_SECS = 30;
    private static final int MAX_REDIRECTS = 10;

    static Observable<String> getGetResponse(String host, int port, String uri) {
        return new CompositeHttpClientBuilder<ByteBuf, ByteBuf>()
                .appendPipelineConfigurator(
                        new PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>>() {
                            @Override
                            public void configureNewPipeline(ChannelPipeline pipeline) {
                                pipeline.addLast("introspecting-handler", new ChannelDuplexHandler() {
                                    private String uri = "<undefined>";

                                    @Override
                                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                                            throws Exception {
                                        if (msg instanceof HttpRequest) {
                                            HttpRequest request = (HttpRequest) msg;
                                            uri = request.uri();
                                            logger.info("Sending request on channel id: " + ctx.channel().toString() +
                                                    ", request URI: " + uri);
                                        }
                                        super.write(ctx, msg, promise);
                                    }

                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                        if (msg instanceof HttpResponse) {
                                            logger.info("Received response on channel id: " + ctx.channel().toString() +
                                                    ", request URI: " + uri);
                                        }
                                        super.channelRead(ctx, msg);
                                    }
                                });

                                try {
                                    int maxContentLength = 10 * 1024 * 1024; // Ten megabytes
                                    pipeline.replace(HttpObjectAggregator.class, "http-object-aggregator",
                                            new HttpObjectAggregator(maxContentLength));
                                } catch (NoSuchElementException ex) {
                                    logger.error("HttpObjectAggregator did not exist in this pipeline. Error: {}",
                                            ex.getMessage(), ex);
                                } catch (IllegalArgumentException ex) {
                                    logger.error("ChannelHandler named http-object-aggregator already existed in this" +
                                            " pipeline. Error: {}", ex.getMessage(), ex);
                                }
                                catch (Throwable t) {
                                    logger.error("Unknown error adding HttpObjectAggregator to Master Client " +
                                            "Pipeline. Error: {}", t.getMessage(), t);
                                }
                            }

                        })
                .build()
                .submit(new RxClient.ServerInfo(host, port),
                        HttpClientRequest.createGet(uri),
                        new HttpClient.HttpClientConfig.Builder().setFollowRedirect(true).followRedirect(MAX_REDIRECTS).build())
                .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>>() {
                    @Override
                    public Observable<ByteBuf> call(HttpClientResponse<ByteBuf> response) {
                        return response.getContent();
                    }
                })
                .map(new Func1<ByteBuf, String>() {
                    @Override
                    public String call(ByteBuf o) {
                        return o.toString(Charset.defaultCharset());
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        logger.warn("Error: " + throwable.getMessage(), throwable);
                    }
                })
                .timeout(GET_TIMEOUT_SECS, TimeUnit.SECONDS);
    }
}
