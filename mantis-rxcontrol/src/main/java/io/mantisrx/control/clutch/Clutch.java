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

import io.mantisrx.control.IActuator;
import io.mantisrx.control.clutch.metrics.IClutchMetricsRegistry;
import io.mantisrx.control.controllers.ControlLoop;
import io.mantisrx.shaded.com.google.common.util.concurrent.AtomicDouble;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import rx.Observable;

/**
 * Clutch is our domain specific autoscaler which adopts many elements from control theory but allows us to fully
 * encapsulate our desired autoscaling behavior.
 *
 * - Multiple Metric handled automatically identifying the dominant metric.
 * - Handles dampening to prevent oscillation.
 * - Handles a resistance metric if users want to provide feedback to the scaler itself.
 **/
public class Clutch implements Observable.Transformer<Event, Object> {

    /** Specifies all the Metrics clutch is capable of dealing with. */
    public enum Metric {
        /** CPU Resource Metric. */
        CPU,
        /** Memory Resource Metric. */
        MEMORY,
        /** Network Resource Metric. */
        NETWORK,
        /** Messages left unpolled, typically Kafka lag. */
        LAG,
        /** Messages dropped. */
        DROPS,
        /** Hypothetical metric which causes the controller to slow down. Currently unused. */
        RESISTANCE,
        /** A user defined resource metric provided by the job under control. Receives priority. */
        UserDefined,
        /** A measure of requests per second handled by the target. */
        RPS,
        /** Messages dropped by by the source job when sending to current job. */
        SOURCEJOB_DROP
    }

    private final IActuator actuator;
    private final AtomicLong initialSize;
    private final Integer minSize;
    private final Integer maxSize;
    private final AtomicDouble dampener;
    private Integer loggingIntervalMins = 60;
    private final Observable<Long> timer = Observable.interval(1, TimeUnit.DAYS).share();

    /**
     * Constructs a new Clutch instance for autoscaling.
     * @param actuator A function of Double -> Double which causes the scaling to occur.
     * @param initialSize The initial size of the cluster as it exists before scaling.
     * @param minSize The minimum size to which the cluster can/should scale.
     * @param maxSize The maximum size to which the cluster can/shoulds cale.
     */
    public Clutch(IActuator actuator, Integer initialSize, Integer minSize, Integer maxSize) {
        this.actuator = actuator;
        this.initialSize = new AtomicLong(initialSize);
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.dampener = new AtomicDouble(1.0);
    }

    public Clutch(IActuator actuator, Integer initialSize,
        Integer minSize, Integer maxSize, Integer loggingIntervalMins) {
        this(actuator, initialSize, minSize, maxSize);
        this.loggingIntervalMins = loggingIntervalMins;
    }

    @Override
    public Observable<Object> call(Observable<Event> eventObservable) {
        final Observable<Event> events = eventObservable.share();

        return events
                .compose(new ClutchConfigurator(new IClutchMetricsRegistry() { }, minSize,
                      maxSize, timer, this.loggingIntervalMins))
                .flatMap(config -> events.compose(new ControlLoop(config, this.actuator,
                        this.initialSize, dampener))
                        .takeUntil(timer)) // takeUntil tears down this control loop when a new config is produced.
                .lift(new OscillationDetector(60, x -> this.dampener.set(Math.pow(x, 3))));
    }
}
