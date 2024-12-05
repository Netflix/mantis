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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.reactivx.mantis.operators.DropOperator;
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
        Channel dummyChannel = mock(Channel.class);

        client.trackConnection(dummyChannel);
        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(1, client.connectionTrackerSize());

        client.resetConn();

        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(0, client.connectionTrackerSize());

        // Test can still add more channels after the client gets reset.
        client.trackConnection(dummyChannel);
        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(1, client.connectionTrackerSize());

        client.trackConnection(dummyChannel);
        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(2, client.connectionTrackerSize());
        client.closeConn();

        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(0, client.connectionTrackerSize());

        // Test cannot add more channels after the client is closed.
        client.trackConnection(dummyChannel);
        logger.info("Connection tracker size: {}", client.connectionTrackerSize());
        assertEquals(0, client.connectionTrackerSize());
    }

    @Test
    public void testStreamContentBuffersBeforeDrop() throws Exception {
        int bufferSize = 20;
        int totalEvents = 100;
        SpectatorRegistryFactory.setRegistry(new DefaultRegistry());
        String metricGroupString = "testmetric_buffer";
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
            bufferSize,
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

        testScheduler.advanceTimeBy(totalEvents, TimeUnit.SECONDS);
        subscriber.assertValueCount(1);
        List<MantisServerSentEvent> events = subscriber.getOnNextEvents();
        assertEquals("0", events.get(0).getEventAsString());

        Metrics metrics = MetricsRegistry.getInstance().getMetric(metricGroupId);
        Counter onNextCounter = metrics.getCounter(DropOperator.Counters.onNext.toString());
        Counter droppedCounter = metrics.getCounter(DropOperator.Counters.dropped.toString());
        logger.info("next: {}", onNextCounter.value());
        logger.info("drop: {}", droppedCounter.value());
        assertTrue(onNextCounter.value() >= bufferSize); // Should request at least the buffer even though we requested 1.
        assertTrue(droppedCounter.value() <= totalEvents - bufferSize ); // We should not drop any of the buffer.
    }
}
