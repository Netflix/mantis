package com.netflix.control.clutch;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.control.IActuator;
import io.vavr.Tuple;
import org.junit.Test;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ExperimentalControlLoopTest {

    @Test
    public void shouldCallActuator() throws Exception {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .metric(Clutch.Metric.RPS)
                .setPoint(100.0)
                .kp(1.0)
                .ki(0)
                .kd(0)
                .minSize(1)
                .maxSize(1000)
                .rope(Tuple.of(0.0, 0.0))
                .cooldownInterval(0)
                .cooldownUnits(TimeUnit.SECONDS)
                .build();
        TestActuator actuator = new TestActuator();
        CountDownLatch latch = actuator.createLatch();

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, 100.0,
                new AtomicDouble(1.0), Observable.timer(10, TimeUnit.MINUTES), Observable.just(100),
                new ExperimentalControlLoop.DefaultRpsMetricComputer(),
                new ExperimentalControlLoop.DefaultScaleComputer());

        PublishSubject<Event> publisher = PublishSubject.create();
        controlLoop.call(publisher).subscribe();

        publisher.onNext(new Event(Clutch.Metric.RPS, 110));
        latch.await();
        assertEquals(110, actuator.lastValue, 1e-10);

        latch = actuator.createLatch();
        publisher.onNext(new Event(Clutch.Metric.RPS, 120));
        latch.await();
        assertEquals(130, actuator.lastValue, 1e-10);

        latch = actuator.createLatch();
        publisher.onNext(new Event(Clutch.Metric.RPS, 90));
        latch.await();
        assertEquals(120, actuator.lastValue, 1e-10);

        latch = actuator.createLatch();
        publisher.onNext(new Event(Clutch.Metric.RPS, 0));
        latch.await();
        assertEquals(20, actuator.lastValue, 1e-10);

        latch = actuator.createLatch();
        publisher.onNext(new Event(Clutch.Metric.RPS, 0));
        latch.await();
        assertEquals(1, actuator.lastValue, 1e-10);

        latch = actuator.createLatch();
        publisher.onNext(new Event(Clutch.Metric.RPS, 2000));
        latch.await();
        assertEquals(1000, actuator.lastValue, 1e-10);
    }

    @Test
    public void testLagDerivativeInMetricComputer() throws Exception {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .metric(Clutch.Metric.RPS)
                .setPoint(100.0)
                .kp(1.0)
                .ki(0)
                .kd(0)
                .minSize(1)
                .maxSize(1000)
                .rope(Tuple.of(0.0, 0.0))
                .cooldownInterval(0)
                .cooldownUnits(TimeUnit.SECONDS)
                .build();
        TestActuator actuator = new TestActuator();
        CountDownLatch latch = actuator.createLatch();

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, 100.0,
                new AtomicDouble(1.0), Observable.timer(10, TimeUnit.MINUTES), Observable.just(100),
                new ExperimentalControlLoop.DefaultRpsMetricComputer(),
                new ExperimentalControlLoop.DefaultScaleComputer());

        PublishSubject<Event> publisher = PublishSubject.create();
        controlLoop.call(publisher).subscribe();

        publisher.onNext(new Event(Clutch.Metric.RPS, 110));
        latch.await();
        assertEquals(110, actuator.lastValue, 1e-10);

        latch = actuator.createLatch();
        publisher.onNext(new Event(Clutch.Metric.LAG, 20));
        publisher.onNext(new Event(Clutch.Metric.RPS, 110));
        latch.await();
        assertEquals(140, actuator.lastValue, 1e-10);

        latch = actuator.createLatch();
        publisher.onNext(new Event(Clutch.Metric.LAG, 10));
        publisher.onNext(new Event(Clutch.Metric.RPS, 100));
        latch.await();
        assertEquals(130, actuator.lastValue, 1e-10);
    }

    @Test
    public void shouldIntegrateErrorDuringCoolDown() throws Exception {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .metric(Clutch.Metric.RPS)
                .setPoint(100.0)
                .kp(1.0)
                .ki(0)
                .kd(0)
                .minSize(1)
                .maxSize(1000)
                .rope(Tuple.of(0.0, 0.0))
                .cooldownInterval(10)
                .cooldownUnits(TimeUnit.MINUTES)
                .build();
        TestActuator actuator = new TestActuator();
        CountDownLatch latch = actuator.createLatch();

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, 100.0,
                new AtomicDouble(1.0), Observable.timer(10, TimeUnit.MINUTES), Observable.just(100),
                new ExperimentalControlLoop.DefaultRpsMetricComputer(),
                new ExperimentalControlLoop.DefaultScaleComputer());

        PublishSubject<Event> publisher = PublishSubject.create();
        controlLoop.call(publisher).subscribe();

        publisher.onNext(new Event(Clutch.Metric.RPS, 110));
        assertEquals(1, latch.getCount());

        publisher.onNext(new Event(Clutch.Metric.RPS, 120));
        assertEquals(1, latch.getCount());

        controlLoop.setCooldownMillis(0);
        publisher.onNext(new Event(Clutch.Metric.RPS, 90));
        latch.await();
        assertEquals(120, actuator.lastValue, 1e-10);
    }

    @Test
    public void shouldIntegrateErrorWithDecay() throws Exception {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .metric(Clutch.Metric.RPS)
                .setPoint(100.0)
                .kp(1.0)
                .ki(0)
                .kd(0)
                .integralDecay(0.9)
                .minSize(1)
                .maxSize(1000)
                .rope(Tuple.of(0.0, 0.0))
                .cooldownInterval(10)
                .cooldownUnits(TimeUnit.MINUTES)
                .build();
        TestActuator actuator = new TestActuator();
        CountDownLatch latch = actuator.createLatch();

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, 100.0,
                new AtomicDouble(1.0), Observable.timer(10, TimeUnit.MINUTES), Observable.just(100),
                new ExperimentalControlLoop.DefaultRpsMetricComputer(),
                new ExperimentalControlLoop.DefaultScaleComputer());

        PublishSubject<Event> publisher = PublishSubject.create();
        controlLoop.call(publisher).subscribe();

        publisher.onNext(new Event(Clutch.Metric.RPS, 110));
        assertEquals(1, latch.getCount());

        publisher.onNext(new Event(Clutch.Metric.RPS, 120));
        assertEquals(1, latch.getCount());

        controlLoop.setCooldownMillis(0);
        publisher.onNext(new Event(Clutch.Metric.RPS, 90));
        latch.await();
        assertEquals(116.1, actuator.lastValue, 1e-10);
    }

    public static class TestActuator extends IActuator {
        private double lastValue;
        private CountDownLatch latch;

        public CountDownLatch createLatch() {
            this.latch = new CountDownLatch(1);
            return this.latch;
        }

        @Override
        protected Double processStep(Double value) {
            this.lastValue = value;
            latch.countDown();
            return value;
        }
    }
}
