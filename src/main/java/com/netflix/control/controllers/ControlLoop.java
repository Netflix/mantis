/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.control.controllers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.control.IActuator;
import com.netflix.control.clutch.Clutch;
import com.netflix.control.clutch.ClutchConfiguration;
import com.netflix.control.clutch.Event;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.schedulers.Schedulers;


@Slf4j
public class ControlLoop implements Observable.Transformer<Event, Double>  {

    private final ClutchConfiguration config;
    private final IActuator actuator;
    private final Double initialSize;
    private final AtomicDouble dampener;
    private final AtomicLong currentScale;
    private final long cooldownMillis;
    private final AtomicLong cooldownTimestamp;
    private Integer loggingIntervalMins = 60;

    private final UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().setK(1024).build();

    public ControlLoop(ClutchConfiguration config, IActuator actuator, Double initialSize) {
        this(config, actuator, initialSize, new AtomicDouble(1.0));
    }

    public ControlLoop(ClutchConfiguration config, IActuator actuator, Double initialSize,
                       AtomicDouble dampener) {
        this.config = config;
        this.actuator = actuator;
        this.initialSize = initialSize;
        this.currentScale = new AtomicLong(Math.round(initialSize));
        this.dampener = dampener;
        this.cooldownMillis = config.getCooldownUnits().toMillis(config.cooldownInterval);
        this.cooldownTimestamp = new AtomicLong(System.currentTimeMillis() + this.cooldownMillis);
    }

    public ControlLoop(ClutchConfiguration config, IActuator actuator, Double initialSize,
                       AtomicDouble dampener, Integer loggingIntervalMins) {
        this(config, actuator, initialSize, dampener);
        this.loggingIntervalMins = loggingIntervalMins;
    }

    private void logSketchSummary(String name, UpdateDoublesSketch sketch) {
        log.info("{} sketch ({}) min: {}, max: {}, median: {}, 99th: {}", name, sketch.getN(), sketch.getMinValue(), sketch.getMaxValue(), sketch.getQuantile(0.5), sketch.getQuantile(0.99));
    }


    @Override
    public Observable<Double> call(Observable<Event> events) {

        Observable<Object> logs = Observable.interval(this.loggingIntervalMins, TimeUnit.MINUTES)
                .observeOn(Schedulers.newThread())
                .map(__ -> {
                    logSketchSummary("ControlLoop " + this.config.metric.toString(), sketch);
                    return null;
                });

        return events
                .filter(e -> e.metric == config.metric)
                .map(e -> e.value)
                .doOnNext(sketch::update)
                .lift(new ErrorComputer(config.setPoint, true, config.rope._1, config.rope._2))
                .lift(PIDController.of(config.kp, config.ki, config.kd))
                .lift(new Integrator(initialSize, config.minSize, config.maxSize))
                .filter(__ -> this.cooldownMillis == 0 || cooldownTimestamp.get() <= System.currentTimeMillis() - this.cooldownMillis)
                .filter(scale -> this.currentScale.get() != Math.round(Math.ceil(scale)))
                .lift(actuator)
                .doOnNext(scale -> this.currentScale.set(Math.round(Math.ceil(scale))))
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()));
    }
}
