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

package io.mantisrx.control.controllers;

import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.mantisrx.control.IActuator;
import io.mantisrx.control.clutch.Clutch;
import io.mantisrx.control.clutch.ClutchConfiguration;
import io.mantisrx.control.clutch.Event;
import io.mantisrx.shaded.com.google.common.util.concurrent.AtomicDouble;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


@Slf4j
public class ControlLoop implements Observable.Transformer<Event, Double>  {

    private final ClutchConfiguration config;
    private final IActuator actuator;
    private final AtomicDouble dampener;
    private final AtomicLong currentScale;
    private final long cooldownMillis;
    private final AtomicLong cooldownTimestamp;

    private final UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().setK(1024).build();

    public ControlLoop(ClutchConfiguration config, IActuator actuator, AtomicLong initialSize) {
        this(config, actuator, initialSize, new AtomicDouble(1.0));
    }

    public ControlLoop(ClutchConfiguration config, IActuator actuator, AtomicLong initialSize,
                       AtomicDouble dampener) {
        this.config = config;
        this.actuator = actuator;
        this.currentScale = initialSize;
        this.dampener = dampener;
        this.cooldownMillis = config.getCooldownUnits().toMillis(config.cooldownInterval);
        this.cooldownTimestamp = new AtomicLong(System.currentTimeMillis() + this.cooldownMillis);
    }

    @Override
    public Observable<Double> call(Observable<Event> events) {
        events = events.share();

        // TODO: How do I get a zero if nothing came through?
        Observable<Event> lag = events.filter(event -> event.getMetric() == Clutch.Metric.LAG);
        Observable<Event> drops = events.filter(event -> event.getMetric() == Clutch.Metric.DROPS);

        return events
                .filter(e -> e.metric == config.metric)
                .map(e -> e.value)
                .doOnNext(sketch::update)
                .lift(new ErrorComputer(config.setPoint, true, config.rope._1, config.rope._2))
                .lift(new PIDController(config.kp, config.ki, config.kd, 1.0, new AtomicDouble(1.0), config.integralDecay))
                .lift(new Integrator(currentScale.get(), config.minSize, config.maxSize, config.integralDecay))
                .filter(__ -> this.cooldownMillis == 0 || cooldownTimestamp.get() <= System.currentTimeMillis() - this.cooldownMillis)
                .filter(scale -> this.currentScale.get() != Math.round(Math.ceil(scale)))
                .lift(actuator)
                .doOnNext(scale -> this.currentScale.set(Math.round(Math.ceil(scale))))
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()));
    }
}
