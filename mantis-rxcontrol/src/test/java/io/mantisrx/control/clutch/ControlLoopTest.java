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

import static org.assertj.core.api.Assertions.assertThat;

import io.mantisrx.control.IActuator;
import io.mantisrx.control.controllers.ControlLoop;
import io.vavr.Tuple;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;


public class ControlLoopTest {

    @Test public void shouldRemainInSteadyState() {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .cooldownInterval(10L)
                .cooldownUnits(TimeUnit.MILLISECONDS)
                .metric(Clutch.Metric.CPU)
                .kd(0.01)
                .kp(0.01)
                .kd(0.01)
                .maxSize(10)
                .minSize(3)
                .rope(Tuple.of(0.25, 0.0))
                .setPoint(0.6)
                .build();

        TestSubscriber<Double> subscriber = new TestSubscriber<>();

        Observable.range(0, 1000)
                .map(__ -> new Event(Clutch.Metric.CPU, 0.5))
                .compose(new ControlLoop(config, IActuator.of(x -> x), new AtomicLong(8)))
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertNoErrors();
        subscriber.assertCompleted();
        assertThat(subscriber.getOnNextEvents()).allSatisfy(x -> assertThat(x).isEqualTo(8.0));
    }

        @Test public void shouldBeUnperturbedByOtherMetrics() {
            ClutchConfiguration config = ClutchConfiguration.builder()
                    .cooldownInterval(10L)
                    .cooldownUnits(TimeUnit.MILLISECONDS)
                    .metric(Clutch.Metric.CPU)
                    .kd(0.01)
                    .kp(0.01)
                    .kd(0.01)
                    .maxSize(10)
                    .minSize(3)
                    .rope(Tuple.of(0.25, 0.0))
                    .setPoint(0.6)
                    .build();

            TestSubscriber<Double> subscriber = new TestSubscriber<>();

            Observable<Event> cpu = Observable.range(0, 1000)
                    .map(__ -> new Event(Clutch.Metric.CPU, 0.5));

            Observable<Event> network = Observable.range(0, 1000)
                    .map(__ -> new Event(Clutch.Metric.NETWORK, 0.1));

            cpu.mergeWith(network)
                    .compose(new ControlLoop(config, IActuator.of(x -> x), new AtomicLong(8)))
                    .toBlocking()
                    .subscribe(subscriber);

            subscriber.assertNoErrors();
            subscriber.assertCompleted();
            assertThat(subscriber.getOnNextEvents()).allSatisfy(x -> assertThat(x).isEqualTo(8.0));
    }

    @Test public void shouldScaleUp() {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .cooldownInterval(10L)
                .cooldownUnits(TimeUnit.MILLISECONDS)
                .metric(Clutch.Metric.CPU)
                .kd(0.01)
                .kp(0.01)
                .kd(0.01)
                .maxSize(10)
                .minSize(3)
                .rope(Tuple.of(0.25, 0.0))
                .setPoint(0.6)
                .build();

        TestSubscriber<Double> subscriber = new TestSubscriber<>();

        Observable.range(0, 1000)
                .map(__ -> new Event(Clutch.Metric.CPU, 0.7))
                .compose(new ControlLoop(config, IActuator.of(Math::ceil), new AtomicLong(8)))
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertNoErrors();
        subscriber.assertCompleted();
        assertThat(subscriber.getOnNextEvents()).allSatisfy(x -> assertThat(x).isEqualTo(9.0));
    }

    @Test public void shouldScaleDown() {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .cooldownInterval(10L)
                .cooldownUnits(TimeUnit.MILLISECONDS)
                .metric(Clutch.Metric.CPU)
                .kd(0.01)
                .kp(0.01)
                .kd(0.01)
                .maxSize(10)
                .minSize(3)
                .rope(Tuple.of(0.25, 0.0))
                .setPoint(0.6)
                .build();

        TestSubscriber<Double> subscriber = new TestSubscriber<>();

        Observable.range(0, 1000)
                .map(__ -> new Event(Clutch.Metric.CPU, 0.2))
                .compose(new ControlLoop(config, IActuator.of(Math::ceil), new AtomicLong(8)))
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertNoErrors();
        subscriber.assertCompleted();
        assertThat(subscriber.getOnNextEvents()).allSatisfy(x -> assertThat(x).isLessThan(8.0));
    }

    @Test public void shouldScaleUpAndDown() {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .cooldownInterval(0L)
                .cooldownUnits(TimeUnit.MILLISECONDS)
                .metric(Clutch.Metric.CPU)
                .kd(0.1)
                .kp(0.5)
                .kd(0.1)
                .maxSize(10)
                .minSize(3)
                .rope(Tuple.of(0.0, 0.0))
                .setPoint(0.6)
                .build();

        TestSubscriber<Double> subscriber = new TestSubscriber<>();

        Observable.range(0, 1000)
                .map(tick -> new Event(Clutch.Metric.CPU, ((tick % 60.0) + 30.0) / 100.0))
                .compose(new ControlLoop(config, IActuator.of(Math::ceil), new AtomicLong(5)))
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertNoErrors();
        subscriber.assertCompleted();
        assertThat(subscriber.getOnNextEvents()).anySatisfy(x -> assertThat(x).isLessThan(5.0));
        assertThat(subscriber.getOnNextEvents()).anySatisfy(x -> assertThat(x).isGreaterThan(5.0));
    }

    @Test public void shouldScaleUpAndDownWithValuesInDifferentRange() {
        ClutchConfiguration config = ClutchConfiguration.builder()
                .cooldownInterval(0L)
                .cooldownUnits(TimeUnit.MILLISECONDS)
                .metric(Clutch.Metric.CPU)
                .kd(0.01)
                .kp(0.05)
                .kd(0.01)
                .maxSize(10)
                .minSize(3)
                .rope(Tuple.of(0.0, 0.0))
                .setPoint(60.0)
                .build();

        TestSubscriber<Double> subscriber = new TestSubscriber<>();

        Observable.range(0, 1000)
                .map(tick -> new Event(Clutch.Metric.CPU, (tick % 60.0) + 30.0))
                .compose(new ControlLoop(config, IActuator.of(Math::ceil), new AtomicLong(5)))
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertNoErrors();
        subscriber.assertCompleted();
        assertThat(subscriber.getOnNextEvents()).anySatisfy(x -> assertThat(x).isLessThan(5.0));
        assertThat(subscriber.getOnNextEvents()).anySatisfy(x -> assertThat(x).isGreaterThan(5.0));
    }
}
