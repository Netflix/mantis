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

import io.mantisrx.connector.publish.core.QueryRegistry;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import rx.subjects.Subject;


public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    private final QueryRegistry registry;
    private final Subject<String, String> eventSubject;
    private static final int DEFAULT_MAX_INITIAL_LENGTH = 4096;
    private static final int DEFAULT_MAX_HEADER_SIZE = 16384;
    private static final int DEFAULT_MAX_CHUNK_SIZE = 32768;
    private static final int DEFAULT_MAX_CONTENT_LENGTH = 1048576;

    public HttpServerInitializer(QueryRegistry registry, Subject<String, String> eventSubject) {
        this.registry = registry;
        this.eventSubject = eventSubject;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();

        p.addLast("http", new HttpServerCodec(DEFAULT_MAX_INITIAL_LENGTH, DEFAULT_MAX_HEADER_SIZE, DEFAULT_MAX_CHUNK_SIZE));
        p.addLast("inflater", new HttpContentDecompressor());
        p.addLast("aggregator", new HttpObjectAggregator(DEFAULT_MAX_CONTENT_LENGTH));

        p.addLast(new HttpSourceServerHandler(registry, eventSubject));
        p.addLast(new NettyExceptionHandler());
    }
}
