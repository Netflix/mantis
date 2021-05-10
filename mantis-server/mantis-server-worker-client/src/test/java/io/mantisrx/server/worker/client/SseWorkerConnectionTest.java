package io.mantisrx.server.worker.client;

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.common.metrics.spectator.MetricId;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SseWorkerConnectionTest {
    private static final Logger logger = LoggerFactory.getLogger(SseWorkerConnectionTest.class);

    @Test
    public void testStreamContentDrops() throws Exception {
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
        Metrics metrics = mock(Metrics.class);
        Counter onNextCounter = new CounterImpl(new MetricId(metricGroupString, DropOperator.Counters.onNext.toString()));
        Counter onErrorCounter = new CounterImpl(new MetricId(metricGroupString, DropOperator.Counters.onError.toString()));
        Counter onCompleteCounter = new CounterImpl(new MetricId(metricGroupString, DropOperator.Counters.onComplete.toString()));
        Counter droppedCounter = new CounterImpl(new MetricId(metricGroupString, DropOperator.Counters.dropped.toString()));
        when(metrics.getMetricGroupId()).thenReturn(metricGroupId);
        when(metrics.getCounter(DropOperator.Counters.onNext.toString())).thenReturn(onNextCounter);
        when(metrics.getCounter(DropOperator.Counters.onError.toString())).thenReturn(onErrorCounter);
        when(metrics.getCounter(DropOperator.Counters.onComplete.toString())).thenReturn(onCompleteCounter);
        when(metrics.getCounter(DropOperator.Counters.dropped.toString())).thenReturn(droppedCounter);
        MetricsRegistry.getInstance().registerAndGet(metrics);
        TestScheduler testScheduler = Schedulers.test();

        List<ServerSentEvent> content = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            content.add(new ServerSentEvent(Unpooled.copiedBuffer(Integer.toString(i), Charset.defaultCharset())));
        }

        Observable<ServerSentEvent> contentObs = Observable.interval(1, TimeUnit.SECONDS, testScheduler).map(t -> new ServerSentEvent(Unpooled.copiedBuffer(Long.toString(t), Charset.defaultCharset())));

        when(response.getContent()).thenReturn(/*Observable.from(content)*/contentObs);

        TestSubscriber<MantisServerSentEvent> subscriber = new TestSubscriber<>(1);

        workerConnection.streamContent(response, b -> {}, 600, "delimiter").subscribeOn(testScheduler).subscribe(subscriber);

        testScheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.assertValueCount(1);
        logger.info("next: {}", onNextCounter.value());
        logger.info("drop: {}", droppedCounter.value());
        assertTrue(onNextCounter.value() < 10);
        assertTrue(droppedCounter.value() > 90);
    }

    public static class CounterImpl implements Counter {
        private final AtomicLong count = new AtomicLong();
        private final MetricId id;

        public CounterImpl(MetricId id) {
            this.id = id;
        }

        @Override
        public void increment() {
            count.incrementAndGet();
        }

        @Override
        public void increment(long x) {
            count.addAndGet(x);
        }

        @Override
        public long value() {
            return count.get();
        }

        @Override
        public long rateValue() {
            return -1;
        }

        @Override
        public long rateTimeInMilliseconds() {
            return -1;
        }

        @Override
        public String event() {
            return null;
        }

        @Override
        public MetricId id() {
            return id;
        }
    }
}
