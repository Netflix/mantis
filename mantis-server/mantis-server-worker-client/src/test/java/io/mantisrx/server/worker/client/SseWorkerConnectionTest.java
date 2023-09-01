/*
 * Copyright 2021 Netflix, Inc.
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

package io.mantisrx.server.worker.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spectator.api.DefaultRegistry;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.reactivx.mantis.operators.DropOperator;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

public class SseWorkerConnectionTest {
    private static final Logger logger = LoggerFactory.getLogger(SseWorkerConnectionTest.class);

    @Test
    public void testStreamContentDrops() throws Exception {
        SpectatorRegistryFactory.setRegistry(new DefaultRegistry());
        String metricGroupString = "testmetric";
        MetricGroupId metricGroupId = new MetricGroupId(metricGroupString);
        SseWorkerConnection workerConnection = new SseWorkerConnection("connection_type",
                "hostname",
                80,
                b -> {},
                b -> {},
                t -> {},
                600,
                false,
                new CopyOnWriteArraySet<>(),
                1,
                null,
                true,
                metricGroupId);
        HttpClientResponse<ServerSentEvent> response = mock(HttpClientResponse.class);
        TestScheduler testScheduler = Schedulers.test();

        // Events are just "0", "1", "2", ...
        Observable<ServerSentEvent> contentObs = Observable.interval(1, TimeUnit.SECONDS, testScheduler)
                .map(t -> new ServerSentEvent(Unpooled.copiedBuffer(Long.toString(t), Charset.defaultCharset())));

        when(response.getContent()).thenReturn(contentObs);

        TestSubscriber<MantisServerSentEvent> subscriber = new TestSubscriber<>(1);

        workerConnection.streamContent(response, b -> {}, 600, "delimiter").subscribeOn(testScheduler).subscribe(subscriber);

        testScheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.assertValueCount(1);
        List<MantisServerSentEvent> events = subscriber.getOnNextEvents();
        assertEquals("0", events.get(0).getEventAsString());

        Metrics metrics = MetricsRegistry.getInstance().getMetric(metricGroupId);
        Counter onNextCounter = metrics.getCounter(DropOperator.Counters.onNext.toString());
        Counter droppedCounter = metrics.getCounter(DropOperator.Counters.dropped.toString());
        logger.info("next: {}", onNextCounter.value());
        logger.info("drop: {}", droppedCounter.value());
        assertTrue(onNextCounter.value() < 10);
        assertTrue(droppedCounter.value() > 90);
    }

    // Goals of tests:
    // SseWorkerConnection uses the MantisHttpClientImpl client
    // Connection tracking functionality of the MantisHttpClientImpl
    // The connect call should go via MantisHttpClientImpl
    @Test
    public void testMantisHttpClientUsage() throws Exception {
        SpectatorRegistryFactory.setRegistry(new DefaultRegistry());
        String metricGroupString = "testmetric";
        MetricGroupId metricGroupId = new MetricGroupId(metricGroupString);
        SseWorkerConnection workerConnection = new SseWorkerConnection("connection_type",
                "hostname",
                80,
                b -> {},
                b -> {},
                t -> {},
                600,
                false,
                new CopyOnWriteArraySet<>(),
                1,
                null,
                true,
                metricGroupId);


        // get SseWorkerConnection to instantiate client
        workerConnection.call();
        MantisHttpClientImpl client = (MantisHttpClientImpl) workerConnection.client;

        // SseWorkerConnection object should use MantisHttpClientImpl
        logger.info("Client type: {}", client.getClass().toString());
        assertTrue(workerConnection.client instanceof MantisHttpClientImpl);


        // check MantisHttpClientImpl.connect usage
        logger.info("MantisHttpClientImpl.connect() called: {}", client.isObservableConectionSet());
        assertTrue(client.isObservableConectionSet());


        // check connection monitor functionality of MantisHttpClientImpl
        Channel dummyChannel = new Channel() {
            @Override
            public ChannelId id() {
                return null;
            }

            @Override
            public EventLoop eventLoop() {
                return null;
            }

            @Override
            public Channel parent() {
                return null;
            }

            @Override
            public ChannelConfig config() {
                return null;
            }

            @Override
            public boolean isOpen() {
                return false;
            }

            @Override
            public boolean isRegistered() {
                return false;
            }

            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public ChannelMetadata metadata() {
                return null;
            }

            @Override
            public SocketAddress localAddress() {
                return null;
            }

            @Override
            public SocketAddress remoteAddress() {
                return null;
            }

            @Override
            public ChannelFuture closeFuture() {
                return null;
            }

            @Override
            public boolean isWritable() {
                return false;
            }

            @Override
            public long bytesBeforeUnwritable() {
                return 0;
            }

            @Override
            public long bytesBeforeWritable() {
                return 0;
            }

            @Override
            public Unsafe unsafe() {
                return null;
            }

            @Override
            public ChannelPipeline pipeline() {
                return null;
            }

            @Override
            public ByteBufAllocator alloc() {
                return null;
            }

            @Override
            public Channel read() {
                return null;
            }

            @Override
            public Channel flush() {
                return null;
            }

            @Override
            public ChannelFuture bind(SocketAddress localAddress) {
                return null;
            }

            @Override
            public ChannelFuture connect(SocketAddress remoteAddress) {
                return null;
            }

            @Override
            public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
                return null;
            }

            @Override
            public ChannelFuture disconnect() {
                return null;
            }

            @Override
            public ChannelFuture close() {
                return null;
            }

            @Override
            public ChannelFuture deregister() {
                return null;
            }

            @Override
            public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture disconnect(ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture close(ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture deregister(ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture write(Object msg) {
                return null;
            }

            @Override
            public ChannelFuture write(Object msg, ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
                return null;
            }

            @Override
            public ChannelFuture writeAndFlush(Object msg) {
                return null;
            }

            @Override
            public ChannelPromise newPromise() {
                return null;
            }

            @Override
            public ChannelProgressivePromise newProgressivePromise() {
                return null;
            }

            @Override
            public ChannelFuture newSucceededFuture() {
                return null;
            }

            @Override
            public ChannelFuture newFailedFuture(Throwable cause) {
                return null;
            }

            @Override
            public ChannelPromise voidPromise() {
                return null;
            }

            @Override
            public <T> Attribute<T> attr(AttributeKey<T> key) {
                return null;
            }

            @Override
            public <T> boolean hasAttr(AttributeKey<T> key) {
                return false;
            }

            @Override
            public int compareTo(Channel o) {
                return 0;
            }
        };

        client.trackConnection(dummyChannel);
        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(1, client.connectionTrackerSize());

        client.closeConn();

        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(0, client.connectionTrackerSize());
        logger.info("Channel active: {}, channel open: {}, channel writable: {}", dummyChannel.isActive(), dummyChannel.isOpen(), dummyChannel.isWritable());
        assertFalse(dummyChannel.isActive());
        assertFalse(dummyChannel.isWritable());
        assertFalse(dummyChannel.isOpen());
    }
}
