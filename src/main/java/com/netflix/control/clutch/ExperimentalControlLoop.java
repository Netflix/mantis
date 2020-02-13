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

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.control.IActuator;
import com.netflix.control.controllers.ErrorComputer;
import com.netflix.control.controllers.Integrator;
import com.netflix.control.controllers.PIDController;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.vavr.Tuple;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


@Slf4j
public class ExperimentalControlLoop implements Observable.Transformer<Event, Double> {

    private final ClutchConfiguration config;
    private final IActuator actuator;
    private final Double initialSize;
    private final AtomicDouble dampener;
    private final long cooldownMillis;


    private final AtomicLong cooldownTimestamp;
    private final AtomicLong currentScale;
    private final Observable<Integer> size;

    private final UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().setK(1024).build();

    public ExperimentalControlLoop(ClutchConfiguration config, IActuator actuator, Double initialSize, Observable<Integer> size) {
        this(config, actuator, initialSize, new AtomicDouble(1.0), size);
    }

    public ExperimentalControlLoop(ClutchConfiguration config, IActuator actuator, Double initialSize,
                                   AtomicDouble dampener, Observable<Integer> size) {
        this.config = config;
        this.actuator = actuator;
        this.initialSize = initialSize;
        this.dampener = dampener;
        this.cooldownMillis = config.getCooldownUnits().toMillis(config.cooldownInterval);

        this.cooldownTimestamp = new AtomicLong(System.currentTimeMillis() + this.cooldownMillis);
        this.currentScale = new AtomicLong(Math.round(initialSize));
        this.size = size;

    }

    @Override
    public Observable<Double> call(Observable<Event> events) {
        events = events.share();

        Observable<Event> lag =
                Observable.just(new Event(Clutch.Metric.LAG, 0.0))
                        .mergeWith(events.filter(event -> event.getMetric() == Clutch.Metric.LAG));

        Observable<Event> drops =
                Observable.just(new Event(Clutch.Metric.DROPS, 0.0))
                        .mergeWith(events.filter(event -> event.getMetric() == Clutch.Metric.DROPS));

        Observable<Event> rps = events.filter(event -> event.getMetric() == Clutch.Metric.RPS);

        Integrator integrator = new Integrator(initialSize, config.minSize, config.maxSize);

        size.doOnNext(currentScale::set)
                .doOnNext(integrator::setSum)
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()))
                .doOnNext(n -> log.info("Clutch received new scheduling update with {} workers.", n))
                .subscribe();

        return rps.withLatestFrom(lag, drops, Tuple::of)
                .map(triple -> {
                    return triple._1.value // RPS
                            + (triple._2.value / this.currentScale.get())  // LAG per worker
                            + (triple._3.value / this.currentScale.get()); // DROPS per worker
                })
                .lift(new ErrorComputer(config.setPoint, true, config.rope._1, config.rope._2))
                .lift(PIDController.of(config.kp, config.ki, config.kd))
                .lift(integrator)
                .filter(__ -> this.cooldownMillis == 0 || cooldownTimestamp.get() <= System.currentTimeMillis() - this.cooldownMillis)
                .filter(scale -> this.currentScale.get() != Math.round(Math.ceil(scale)))
                .lift(actuator)
                .doOnNext(scale -> this.currentScale.set(Math.round(Math.ceil(scale))))
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()));
    }
}
