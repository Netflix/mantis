/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.control.clutch;

import static org.junit.Assert.assertEquals;

import io.mantisrx.control.IActuator;
import io.mantisrx.shaded.com.google.common.util.concurrent.AtomicDouble;
import io.vavr.Tuple;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.Test;
import rx.Observable;
import rx.subjects.PublishSubject;

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

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, new AtomicLong(100),
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

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, new AtomicLong(100),
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

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, new AtomicLong(100),
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

        ExperimentalControlLoop controlLoop = new ExperimentalControlLoop(config, actuator, new AtomicLong(100),
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

    @Test
    public void shouldHandleScalingDisabledAndEnabled() throws Exception {
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

        // Test with scaling enabled
        TestActuator actuatorScalingEnabled = new TestActuator(100);
        actuatorScalingEnabled.setScalingDisabled(false); // Simulate scaling being enabled
        CountDownLatch latchScalingEnabled = actuatorScalingEnabled.createLatch();

        AtomicLong currentSizeScalingEnabled = new AtomicLong(100);
        ExperimentalControlLoop controlLoopScalingEnabled = new ExperimentalControlLoop(config, actuatorScalingEnabled, currentSizeScalingEnabled,
            new AtomicDouble(1.0), Observable.timer(10, TimeUnit.MINUTES), Observable.just(100),
            new ExperimentalControlLoop.DefaultRpsMetricComputer(),
            new ExperimentalControlLoop.DefaultScaleComputer());

        PublishSubject<Event> publisherScalingEnabled = PublishSubject.create();
        controlLoopScalingEnabled.call(publisherScalingEnabled).subscribe();

        controlLoopScalingEnabled.setCooldownMillis(0);
        publisherScalingEnabled.onNext(new Event(Clutch.Metric.RPS, 90));
        latchScalingEnabled.await();
        assertEquals(90, currentSizeScalingEnabled.get()); // Verify that current size has changed

        // Test with scaling disabled
        TestActuator actuatorScalingDisabled = new TestActuator(100);
        actuatorScalingDisabled.setScalingDisabled(true); // Simulate scaling being disabled
        CountDownLatch latchScalingDisabled = actuatorScalingDisabled.createLatch();

        AtomicLong currentSizeScalingDisabled = new AtomicLong(100);
        ExperimentalControlLoop controlLoopScalingDisabled = new ExperimentalControlLoop(config, actuatorScalingDisabled, currentSizeScalingDisabled,
            new AtomicDouble(1.0), Observable.timer(10, TimeUnit.MINUTES), Observable.just(100),
            new ExperimentalControlLoop.DefaultRpsMetricComputer(),
            new ExperimentalControlLoop.DefaultScaleComputer());

        PublishSubject<Event> publisherScalingDisabled = PublishSubject.create();
        controlLoopScalingDisabled.call(publisherScalingDisabled).subscribe();

        controlLoopScalingDisabled.setCooldownMillis(0);
        publisherScalingDisabled.onNext(new Event(Clutch.Metric.RPS, 90));
        latchScalingDisabled.await();
        assertEquals(100, currentSizeScalingDisabled.get()); // Verify that current size has not changed
    }

    @NoArgsConstructor
    public static class TestActuator extends IActuator {
        private double lastValue;
        private CountDownLatch latch;
        @Setter
        private boolean scalingDisabled = false; // Flag to simulate scaling is being disabled

        TestActuator(double initialValue) {
            this.lastValue = initialValue;
        }

        public CountDownLatch createLatch() {
            this.latch = new CountDownLatch(1);
            return this.latch;
        }

        @Override
        protected Double processStep(Double value) {
            if (scalingDisabled) {
                latch.countDown();
                System.out.println("lastValue " + lastValue);
                return lastValue; // Return the original value if scaling is disabled
            }
            this.lastValue = value;
            latch.countDown();
            return value;
        }
    }
}
