package io.mantisrx.server.worker.client;

import com.netflix.spectator.api.DefaultRegistry;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.netty.buffer.Unpooled;
import io.reactivx.mantis.operators.DropOperator;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
}
