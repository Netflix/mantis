/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.control.clutch;

import com.netflix.control.IActuator;
import com.netflix.control.clutch.metrics.IClutchMetricsRegistry;
import com.yahoo.sketches.quantiles.DoublesSketch;
import io.vavr.Function1;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import rx.Observable;

import java.util.concurrent.TimeUnit;

/**
 * Clutch experimental is taking a radically different approach to auto scaling, something akin to the first
 * iteration of clutch but with lessons from the past year of auto scaling.
 */
public class ClutchExperimental implements Observable.Transformer<Event, Object> {

    private final IActuator actuator;
    private final Integer initialSize;
    private final Integer minSize;
    private final Integer maxSize;

    private final Observable<Long> timer;
    private final Observable<Integer> size;
    private final long initialConfigMillis;
    private final Function1<DoublesSketch, ClutchConfiguration> configurator;

    /**
     * Constructs a new Clutch instance for autoscaling.
     * @param actuator A function of Double -> Double which causes the scaling to occur.
     * @param initialSize The initial size of the cluster as it exists before scaling.
     * @param minSize The minimum size to which the cluster can/should scale.
     * @param maxSize The maximum size to which the cluster can/should scale.
     * @param size An observable indicating the size of the cluster should external events resize it.
     * @param timer An observable on which each tick signifies a new configuration should be emitted.
     * @param initialConfigMillis The initial number of milliseconds before initial configuration.
     */
    public ClutchExperimental(IActuator actuator, Integer initialSize, Integer minSize, Integer maxSize,
                              Observable<Integer> size, Observable<Long> timer, long initialConfigMillis,
                              Function1<DoublesSketch, ClutchConfiguration> configurator) {
        this.actuator = actuator;
        this.initialSize = initialSize;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.size = size;
        this.timer = timer;
        this.initialConfigMillis = initialConfigMillis;
        this.configurator = configurator;
    }

    public ClutchExperimental(IActuator actuator, Integer initialSize, Integer minSize, Integer maxSize,
                              Observable<Integer> size, Observable<Long> timer, long initialConfigMillis) {

        this(actuator, initialSize, minSize, maxSize, size, timer, initialConfigMillis, (sketch) -> {
            double setPoint = 0.6 * sketch.getQuantile(0.99);
            Tuple2<Double, Double> rope = Tuple.of(setPoint * 0.15, 0.0);

            // TODO: Significant improvements to gain computation can likely be made.
            double kp = (setPoint * 1e-6) / 5.0;
            double ki = 0.0;
            double kd = (setPoint * 1e-6) / 4.0;

            return new ClutchConfiguration.ClutchConfigurationBuilder()
                    .metric(Clutch.Metric.RPS)
                    .setPoint(setPoint)
                    .kp(kp)
                    .ki(ki)
                    .kd(kd)
                    .minSize(minSize)
                    .maxSize(maxSize)
                    .rope(rope)
                    .cooldownInterval(5)
                    .cooldownUnits(TimeUnit.MINUTES)
                    .build();
        });
    }

    @Override
    public Observable<Object> call(Observable<Event> eventObservable) {
        final Observable<Event> events = eventObservable.share();

        return events
                .compose(new ExperimentalClutchConfigurator(new IClutchMetricsRegistry() { }, timer,
                        initialConfigMillis, configurator))
                .flatMap(config -> events
                        .compose(new ExperimentalControlLoop(config, this.actuator,
                                this.initialSize.doubleValue(), timer, size))
                        .takeUntil(timer)); // takeUntil tears down this control loop when a new config is produced.
    }
}
