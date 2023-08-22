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

import io.mantisrx.common.MantisServerSentEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.reactivx.mantis.operators.DropOperator;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.apache.flink.metrics.Meter;
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
        String metricGroupString = "testmetric";
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
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
                metricGroupString,
                meterRegistry);
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

        Counter onNextCounter = meterRegistry.counter(DropOperator.Counters.onNext.toString());
        Counter droppedCounter = meterRegistry.counter(DropOperator.Counters.dropped.toString());
        logger.info("next: {}", onNextCounter.count());
        logger.info("drop: {}", droppedCounter.count());
        assertTrue(onNextCounter.count() < 10);
        assertTrue(droppedCounter.count() > 90);
    }
}
