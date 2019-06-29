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

package io.mantisrx.server.worker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.server.HttpServer;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerRequest;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import mantis.io.reactivex.netty.protocol.http.server.RequestHandler;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;


public class TestSseServerFactory {

    private static final AtomicInteger port = new AtomicInteger(30303);
    private static List<HttpServer<String, ServerSentEvent>> servers = new ArrayList<>();

    private TestSseServerFactory() {}

    public static int getServerPort() {
        return port.incrementAndGet();
    }

    public static int newServerWithInitialData(final String data) {
        int port = getServerPort();
        return newServerWithInitialData(port, data);
    }

    public static int newServerWithInitialData(final int port, final String data) {
        final HttpServer<String, ServerSentEvent> server = RxNetty.newHttpServerBuilder(
                port,
                new RequestHandler<String, ServerSentEvent>() {
                    @Override
                    public Observable<Void> handle(HttpServerRequest<String> req, HttpServerResponse<ServerSentEvent> resp) {
                        final ByteBuf byteBuf = resp.getAllocator().buffer().writeBytes(data.getBytes());
                        resp.writeAndFlush(new ServerSentEvent(byteBuf));
                        return Observable.empty();
                    }
                })
                .pipelineConfigurator(PipelineConfigurators.<String>serveSseConfigurator())
                .channelOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 5 * 1024 * 1024)
                .channelOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 1024 * 1024)
                .build();
        server.start();
        synchronized (servers) {
            servers.add(server);
        }
        return port;
    }

    public static void stopAllRunning() throws InterruptedException {
        synchronized (servers) {
            final Iterator<HttpServer<String, ServerSentEvent>> iter = servers.iterator();
            while (iter.hasNext()) {
                final HttpServer<String, ServerSentEvent> server = iter.next();
                server.shutdown();
                iter.remove();
            }
        }
    }
}