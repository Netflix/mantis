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

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.control.IActuator;
import com.netflix.control.clutch.metrics.IClutchMetricsRegistry;
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


    public ClutchExperimental(IActuator actuator, Integer initialSize, Integer minSize, Integer maxSize, Observable<Integer> size) {
        this(actuator, initialSize, minSize, maxSize, size, Observable.interval(1, TimeUnit.DAYS), 1000 * 60 * 10);
    }

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
                              Observable<Integer> size, Observable<Long> timer, long initialConfigMillis) {
        this.actuator = actuator;
        this.initialSize = initialSize;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.size = size;
        this.timer = timer;
        this.initialConfigMillis = initialConfigMillis;
    }

    @Override
    public Observable<Object> call(Observable<Event> eventObservable) {
        final Observable<Event> events = eventObservable.share();

        return events
                .compose(new ExperimentalClutchConfigurator(new IClutchMetricsRegistry() { }, minSize,
                        maxSize, timer, initialConfigMillis))
                .flatMap(config -> events
                        .compose(new ExperimentalControlLoop(config, this.actuator,
                                this.initialSize.doubleValue(), size))
                        .takeUntil(timer)); // takeUntil tears down this control loop when a new config is produced.
    }
}
